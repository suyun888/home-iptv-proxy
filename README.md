# home-iptv-proxy

一个家庭内使用的 IPTV 订阅聚合、本地转发和网页后台管理服务。

项目已经内置 `xhsuhd` 作为固定上游源，默认会出现在 M3U 源列表 `Source 1`，名称就是 `xhsuhd`。`a1` 和 `web_session` 现在也可以直接在后台页面维护。

## 功能

- 多个 `m3u` 源合并成一个本地 `list.m3u`
- 兼容 `m3u` / `txt` 两种本地订阅输出
- 输出本地频道地址 `/live/{id}`
- 对上游 `m3u8` 播放列表做本地重写，播放器只连你的本地服务
- 定时刷新远程订阅源
- 提供网页后台，可直接增删改 `m3u` 地址并保存生效
- 内置 `xhsuhd` 容器，可直接把 `http://xhsuhd:34567/xhslist.m3u` 作为固定上游源
- 可选内置 `iptv-4gtv-system` 容器，可把 4GTV 输出作为上游源一起整理
- 后台可直接保存 `xhsuhd` 的 `a1` / `web_session`，并一键重新应用到上游容器
- 每条源可单独设置代理地址
- 可整合 XMLTV 节目单，并自动在 `list.m3u` 写入 `x-tvg-url`
- 节目单支持流式抓取、磁盘缓存和 `epg.xml.gz` 压缩输出
- 支持结合节目单创建录制任务，可设置提前/延后分钟数
- 后台显示当前版本号，并支持自动更新状态展示
- 可在后台直接触发手动更新，更新结果会在版本区提示
- `/health` 和 `/channels` 方便排查

## 快速开始

1. 如果要启用 `xhsuhd`，先准备小红书网页登录后的 Cookie，后续也可以直接在后台修改：

```bash
export XHS_A1="你的a1"
export XHS_WEB_SESSION="你的web_session"
```

2. 如果要启用 4GTV，先准备访问 token：

```bash
export FOURGTV_ACCESS_TOKEN="你的Token"
```

默认 compose 会启动 `iptv-4gtv-system`，并预留上游源：

```text
http://iptv-4gtv-system:5050/?type=m3u&token=你的Token&proxy=true
```

首次启动后可在后台把 `4gtv` 这条源改成启用。

3. 首次启动后打开后台页面，填入你自己的订阅源。
4. 把 `signing_secret` 改成一串随机字符串。
5. 启动：

```bash
docker compose up -d
```

6. 导入播放器：

```text
http://你的主机IP:28788/list.m3u
```

7. 打开后台：

```text
http://你的主机IP:28788/admin
```

8. 如果你部署了 `CharmingEPG`，可在后台填写：

```text
节目单地址: http://你的主机IP:30008/all
```

如果 `CharmingEPG` 的 HTTP 接口不可用，也可以把它生成目录挂载进容器，然后直接读取本地 XML：

```text
节目单地址: /epg/tvb/tvb_*.xml
```

## 接口

- `/health`
- `/channels`
- `/m3u`
- `/list.m3u`
- `/txt`
- `/epg.xml.gz`
- `/live/{id}`
- `/epg.xml`
- `/admin`
- `/admin/epg/programmes`

## 说明

- 如果你放在反向代理后面，可以设置 `public_base_url`，例如 `https://tv.example.com`
- `xhsuhd` 需要有效的 `XHS_A1` 和 `XHS_WEB_SESSION`，否则列表可能为空
- 后台保存后会同步写入 `config/xhsuhd.env`；启用一键应用的部署会直接重启 `xhsuhd` 让新 Cookie 生效
- 在当前项目的 Docker 网络里，`xhsuhd` 的默认地址就是 `http://xhsuhd:34567/xhslist.m3u`
- 在当前项目的 Docker 网络里，`iptv-4gtv-system` 的默认订阅地址就是 `http://iptv-4gtv-system:5050/?type=m3u&token=你的Token&proxy=true`
- 如果 `CharmingEPG` 首次启动后 `/all` 暂时不可用，通常是在后台生成合并节目单，等几分钟再试
- `epg_source_url` 支持 `http/https`，也支持容器内本地文件路径和简单通配，例如 `/epg/tvb/tvb_*.xml`
- `epg.xml.gz` 会把节目单缓存到 `epg_cache_dir` 后再输出，适合播放器长期订阅
- 录制任务保存在 `config/recordings.json`，默认输出目录是 `/app/config/recordings`
- Docker 镜像已内置 `ffmpeg`，录制任务会直接调用它抓取本地 `/live/{id}` 流
- 网页后台保存后会直接写入 `config/sources.yaml`，并自动刷新频道列表
- 默认 `docker-compose.yml` 和 `install.sh` 都会启用 `watchtower`，只监控 `home-iptv-proxy` 这一个容器
- 后台版本区的“立即手动更新”依赖 `IPTV_UPDATE_COMMAND`，安装脚本生成的部署会自动注入；同时容器会挂载 Docker Socket 和 compose 文件
- `/health` 现在也会返回 `version`、`auto_update_enabled`、`manual_update_enabled`
