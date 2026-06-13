# home-iptv-proxy

一个家庭内使用的 IPTV 订阅聚合、本地转发和网页后台管理服务。

## 功能

- 多个 `m3u` 源合并成一个本地 `list.m3u`
- 输出本地频道地址 `/live/{id}`
- 对上游 `m3u8` 播放列表做本地重写，播放器只连你的本地服务
- 定时刷新远程订阅源
- 提供网页后台，可直接增删改 `m3u` 地址并保存生效
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

## 接口

- `/health`
- `/channels`
- `/list.m3u`
- `/live/{id}`
- `/admin`

## 说明

- 如果你放在反向代理后面，可以设置 `public_base_url`，例如 `https://tv.example.com`
- 网页后台保存后会直接写入 `config/sources.yaml`，并自动刷新频道列表
