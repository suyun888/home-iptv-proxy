# home-iptv-proxy

一个家庭内使用的 IPTV 订阅聚合与本地转发服务。

## 功能

- 多个 `m3u` 源合并成一个本地 `list.m3u`
- 输出本地频道地址 `/live/{id}`
- 对上游 `m3u8` 播放列表做本地重写，播放器只连你的本地服务
- 定时刷新远程订阅源
- `/health` 和 `/channels` 方便排查

## 快速开始

1. 编辑 `config/sources.yaml`，填入你自己的订阅源。
2. 把 `signing_secret` 改成一串随机字符串。
3. 启动：

```bash
docker compose up -d --build
```

4. 导入播放器：

```text
http://你的主机IP:28787/list.m3u
```

## 接口

- `/health`
- `/channels`
- `/list.m3u`
- `/live/{id}`

## 说明

- 如果你放在反向代理后面，可以设置 `public_base_url`，例如 `https://tv.example.com`
- 当前版本聚焦家庭内统一入口，不包含网页管理后台
