# home-iptv-proxy

一个家庭内使用的 IPTV 订阅聚合、本地转发和网页后台管理服务。

## 功能

- 多个 `m3u` 源合并成一个本地 `list.m3u`
- 兼容 `m3u` / `txt` 两种本地订阅输出
- 输出本地频道地址 `/live/{id}`
- 对上游 `m3u8` 播放列表做本地重写，播放器只连你的本地服务
- 定时刷新远程订阅源
- 提供网页后台，可直接增删改 `m3u` 地址并保存生效
- 每条源可单独设置代理地址
- 可整合 XMLTV 节目单，并自动在 `list.m3u` 写入 `x-tvg-url`
- 节目单支持流式抓取、磁盘缓存和 `epg.xml.gz` 压缩输出
- 支持结合节目单创建录制任务，可设置提前/延后分钟数
- `/health` 和 `/channels` 方便排查

## 快速开始

1. 首次启动后打开后台页面，填入你自己的订阅源。
2. 把 `signing_secret` 改成一串随机字符串。
3. 启动：

```bash
docker compose up -d --build
```

4. 导入播放器：

```text
http://你的主机IP:28788/list.m3u
```

5. 打开后台：

```text
http://你的主机IP:28788/admin
```

6. 如果你部署了 `CharmingEPG`，可在后台填写：

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
- 如果 `CharmingEPG` 首次启动后 `/all` 暂时不可用，通常是在后台生成合并节目单，等几分钟再试
- `epg_source_url` 支持 `http/https`，也支持容器内本地文件路径和简单通配，例如 `/epg/tvb/tvb_*.xml`
- `epg.xml.gz` 会把节目单缓存到 `epg_cache_dir` 后再输出，适合播放器长期订阅
- 录制任务保存在 `config/recordings.json`，默认输出目录是 `/app/config/recordings`
- Docker 镜像已内置 `ffmpeg`，录制任务会直接调用它抓取本地 `/live/{id}` 流
- 网页后台保存后会直接写入 `config/sources.yaml`，并自动刷新频道列表
