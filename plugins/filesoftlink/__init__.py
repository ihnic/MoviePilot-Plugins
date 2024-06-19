import datetime
import os
import re
import shutil
import threading
import traceback
from pathlib import Path
from typing import Dict, Any, Optional

import pytz
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from watchdog.events import FileSystemEventHandler
from watchdog.observers import Observer
from watchdog.observers.polling import PollingObserver

from app import schemas
from app.core.config import settings
from app.core.event import eventmanager, Event
from app.log import logger
from app.plugins import _PluginBase
from app.schemas.types import EventType, MediaType, SystemConfigKey
from app.utils.system import SystemUtils

lock = threading.Lock()


class FileMonitorHandler(FileSystemEventHandler):
    """
    目录监控响应类
    """

    def __init__(self, monpath: str, sync: Any, **kwargs):
        super(FileMonitorHandler, self).__init__(**kwargs)
        self._watch_path = monpath
        self.sync = sync

    def on_created(self, event):
        self.sync.event_handler(event=event, text="创建",
                                mon_path=self._watch_path, event_path=event.src_path)

    def on_moved(self, event):
        self.sync.event_handler(event=event, text="移动",
                                mon_path=self._watch_path, event_path=event.dest_path)

    def on_deleted(self, event):
        self.sync.event_handler(event=event, text="删除",
                                mon_path=self._watch_path, event_path=event.src_path)


class FileSoftLink(_PluginBase):
    # 插件名称
    plugin_name = "实时软连接（魔改版）"
    # 插件描述
    plugin_desc = "监控目录文件变化，媒体文件软连接，其他文件可选复制。"
    # 插件图标
    plugin_icon = "https://raw.githubusercontent.com/thsrite/MoviePilot-Plugins/main/icons/softlink.png"
    # 插件版本
    plugin_version = "1.0"
    # 插件作者
    plugin_author = "nic"
    # 作者主页
    author_url = "https://github.com/ihnic"
    # 插件配置项ID前缀
    plugin_config_prefix = "filesoftlink1_"
    # 加载顺序
    plugin_order = 10
    # 可使用的用户级别
    auth_level = 1

    # 私有属性
    _scheduler = None
    _observer = []
    _enabled = False
    _onlyonce = False
    _copy_files = False
    _cron = None
    _size = 0
    # 模式 compatibility/fast
    _mode = "compatibility"
    _monitor_dirs = ""
    _exclude_keywords = ""
    # 存储源目录与目的目录关系
    _dirconf: Dict[str, Optional[Path]] = {}
    _medias = {}
    # 退出事件
    _event = threading.Event()

    def init_plugin(self, config: dict = None):
        # 清空配置
        self._dirconf = {}

        # 读取配置
        if config:
            self._enabled = config.get("enabled")
            self._onlyonce = config.get("onlyonce")
            self._copy_files = config.get("copy_files")
            self._mode = config.get("mode")
            self._monitor_dirs = config.get("monitor_dirs") or ""
            self._exclude_keywords = config.get("exclude_keywords") or ""
            self._cron = config.get("cron")
            self._size = config.get("size") or 0

        # 停止现有任务
        self.stop_service()

        if self._enabled or self._onlyonce:
            # 定时服务管理器
            self._scheduler = BackgroundScheduler(timezone=settings.TZ)

            # 读取目录配置
            monitor_dirs = self._monitor_dirs.split("\n")
            if not monitor_dirs:
                return
            for mon_path in monitor_dirs:
                # 格式源目录:目的目录
                if not mon_path:
                    continue

                # 存储目的目录
                if SystemUtils.is_windows():
                    if mon_path.count(":") > 1:
                        paths = [mon_path.split(":")[0] + ":" + mon_path.split(":")[1],
                                 mon_path.split(":")[2] + ":" + mon_path.split(":")[3]]
                    else:
                        paths = [mon_path]
                else:
                    paths = mon_path.split(":")

                # 目的目录
                target_path = None
                if len(paths) > 1:
                    mon_path = paths[0]
                    target_path = Path(paths[1])
                    self._dirconf[mon_path] = target_path
                else:
                    self._dirconf[mon_path] = None

                # 启用目录监控
                if self._enabled:
                    # 检查媒体库目录是不是下载目录的子目录
                    try:
                        if target_path and target_path.is_relative_to(Path(mon_path)):
                            logger.warn(f"{target_path} 是监控目录 {mon_path} 的子目录，无法监控")
                            self.systemmessage.put(f"{target_path} 是下载目录 {mon_path} 的子目录，无法监控")
                            continue
                    except Exception as e:
                        logger.debug(str(e))
                        pass

                    # 异步开启云盘监控
                    logger.info(f"异步开启实时硬链接 {mon_path} {self._mode}，延迟5s启动")
                    self._scheduler.add_job(func=self.start_monitor, trigger='date',
                                            run_date=datetime.datetime.now(
                                                tz=pytz.timezone(settings.TZ)) + datetime.timedelta(seconds=5),
                                            name=f"实时硬链接 {mon_path}",
                                            kwargs={
                                                "source_dir": mon_path
                                            })

            # 运行一次定时服务
            if self._onlyonce:
                logger.info("实时软连接服务启动，立即运行一次")
                self._scheduler.add_job(name="实时软连接", func=self.sync_all, trigger='date',
                                        run_date=datetime.datetime.now(
                                            tz=pytz.timezone(settings.TZ)) + datetime.timedelta(seconds=3)
                                        )
                # 关闭一次性开关
                self._onlyonce = False
                # 保存配置
                self.__update_config()

            # 启动定时服务
            if self._scheduler.get_jobs():
                self._scheduler.print_jobs()
                self._scheduler.start()

    def start_monitor(self, source_dir: str):
        """
        异步开启实时软链接
        """
        try:
            if str(self._mode) == "compatibility":
                # 兼容模式，目录同步性能降低且NAS不能休眠，但可以兼容挂载的远程共享目录如SMB
                observer = PollingObserver(timeout=10)
            else:
                # 内部处理系统操作类型选择最优解
                observer = Observer(timeout=10)
            self._observer.append(observer)
            observer.schedule(FileMonitorHandler(source_dir, self), path=source_dir, recursive=True)
            observer.daemon = True
            observer.start()
            logger.info(f"{source_dir} 的实时软链接服务启动")
        except Exception as e:
            err_msg = str(e)
            if "inotify" in err_msg and "reached" in err_msg:
                logger.warn(
                    f"云盘监控服务启动出现异常：{err_msg}，请在宿主机上（不是docker容器内）执行以下命令并重启："
                    + """
                                           echo fs.inotify.max_user_watches=524288 | sudo tee -a /etc/sysctl.conf
                                           echo fs.inotify.max_user_instances=524288 | sudo tee -a /etc/sysctl.conf
                                           sudo sysctl -p
                                           """)
            else:
                logger.error(f"{source_dir} 启动云盘监控失败：{err_msg}")
            self.systemmessage.put(f"{source_dir} 启动云盘监控失败：{err_msg}")

    def __update_config(self):
        """
        更新配置
        """
        self.update_config({
            "enabled": self._enabled,
            "onlyonce": self._onlyonce,
            "copy_files": self._copy_files,
            "mode": self._mode,
            "monitor_dirs": self._monitor_dirs,
            "exclude_keywords": self._exclude_keywords,
            "cron": self._cron,
            "size": self._size
        })

    @eventmanager.register(EventType.PluginAction)
    def remote_sync(self, event: Event):
        """
        远程全量同步
        """
        if event:
            event_data = event.event_data
            if not event_data or event_data.get("action") != "softlink_sync":
                return
            self.post_message(channel=event.event_data.get("channel"),
                              title="开始同步监控目录 ...",
                              userid=event.event_data.get("user"))
        self.sync_all()
        if event:
            self.post_message(channel=event.event_data.get("channel"),
                              title="监控目录同步完成！", userid=event.event_data.get("user"))

    def sync_all(self):
        """
        立即运行一次，全量同步目录中所有文件
        """
        for mon_path, target_path in self._dirconf.items():
            logger.info(f"开始同步监控目录：{mon_path}")
            for path in Path(mon_path).rglob('*'):
                if path.is_file():
                    self.__handle_file(path, mon_path, target_path)

    def event_handler(self, event, text, mon_path, event_path):
        """
        事件处理
        """
        with lock:
            try:
                logger.info(f"{event_path} {text}事件")
                # 跳过云盘临时文件
                if not Path(event_path).exists() and text != "删除":
                    return
                # 跳过大于指定大小的文件
                if self._size > 0 and Path(event_path).exists() and os.path.getsize(event_path) > self._size * 1024 * 1024:
                    logger.info(f"{event_path} 文件大小超过限制，跳过")
                    return

                # 跳过排除的文件或目录
                if self._exclude_keywords and re.findall(self._exclude_keywords, event_path, re.IGNORECASE):
                    logger.info(f"{event_path} 匹配排除关键字，跳过")
                    return

                # 软链接或复制文件
                if text == "删除":
                    self.__handle_delete(event_path, mon_path)
                else:
                    self.__handle_file(event_path, mon_path, self._dirconf.get(mon_path))

            except Exception as e:
                logger.error(f"{event_path} {text}事件处理失败：{str(e)}")
                self.systemmessage.put(f"{event_path} {text}事件处理失败：{str(e)}")

    def __handle_file(self, path: Path, mon_path: str, target_path: Path):
        """
        同步单个文件，创建软链接或复制文件
        """
        # 跳过排除的文件或目录
        if not path.exists():
            return

        # 跳过媒体库目录
        if any([p in str(path) for p in self._dirconf.values() if p]):
            return

        # 创建目的目录
        relpath = path.relative_to(mon_path)
        target_file = target_path.joinpath(relpath)
        target_dir = target_file.parent
        target_dir.mkdir(parents=True, exist_ok=True)

        if target_file.exists():
            target_file.unlink()

        if self._copy_files:
            if path.suffix.lower() in MediaType.ALL_VIDEO + MediaType.ALL_SUBTITLE:
                os.symlink(path, target_file)
                logger.info(f"已为媒体文件 {path} 创建软链接 {target_file}")
            else:
                shutil.copy(path, target_file)
                logger.info(f"已复制文件 {path} 到 {target_file}")
        else:
            os.symlink(path, target_file)
            logger.info(f"已为文件 {path} 创建软链接 {target_file}")

    def __handle_delete(self, path: Path, mon_path: str):
        """
        删除目标目录中的对应文件
        """
        relpath = Path(path).relative_to(mon_path)
        for target_dir in self._dirconf.values():
            if target_dir:
                target_file = target_dir.joinpath(relpath)
                if target_file.exists():
                    target_file.unlink()
                    logger.info(f"已删除文件 {target_file}")

    def stop_service(self):
        """
        停止服务
        """
        self._event.set()
        if self._scheduler:
            self._scheduler.remove_all_jobs()
            self._scheduler.shutdown()
        for observer in self._observer:
            observer.stop()
            observer.join()
        self._observer = []

    def get_state(self):
        pass

    def get_command(self):
        pass

    def get_api(self):
        pass

    def get_service(self):
        pass

    def get_form(self):
        pass

    def get_page(self):
        pass