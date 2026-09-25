# home-iptv-proxy

独立的 M3U 源管理服务。启动时源列表为空；在后台逐条添加源，并为每条源明确选择：

- **直连**：聚合列表保留上游频道播放地址和回放属性。适合 4GTV，以及已经由 GITV Docker 提供固定公开直播和回放入口的源。
- **中转**：聚合列表使用本服务的固定直播地址；如果上游频道带有回放地址，则输出 APTV 可用的本地回放地址。适合需要本服务接管播放流的源。

中转在播放器请求频道或回放时才访问上游。HLS 的子清单、分片、密钥以及 Range 请求会继续中转。频道没有上游回放属性时，不会虚构回放能力。服务不会代替 GITV 或 4GTV 自身的登录、播放鉴权与续期；这些仍由各自的上游服务处理。

## 部署

1. 复制 .env.example 为 .env，设置强密码 IPTV_ADMIN_PASSWORD。
2. 将 config/sources.yaml 的 signing_secret 改为至少 32 字符的随机值。首次启动保持空源列表。
3. 运行 docker compose up -d --build。

默认后台：http://主机地址:28788/admin，用户名 admin，密码为 .env 中的值。

固定订阅：http://主机地址:28788/list.m3u。

后台支持添加、编辑、删除、启停源以及手动刷新，服务还会按 refresh_minutes 定时刷新。每条源都必须指定播放方式。源配置保存在 config/sources.yaml；更新镜像时挂载目录会保留配置。

GITV Docker 的订阅如果已经输出可供播放器访问的固定 `/live/...` 地址，以及带有 APTV `catchup="default"` 和公开 `/catchup/...` 地址的 `catchup-source`，GITV 源就选择“直连”。本服务保留这些频道地址、回放属性和上游 M3U 的 `x-tvg-url`；播放和回放请求直接到达 GITV Docker，由它完成鉴权及续期。4GTV 源也选择“直连”，下发它原来的频道地址。例如：

    name: GITV
    url: http://10.10.10.20:8097/tv.m3u
    mode: direct

GITV 订阅中的频道和回放地址应为播放器可访问的公开入口，例如 `https://gitv.linboqin.cn:16678/live/...` 和 `https://gitv.linboqin.cn:16678/catchup/...`。订阅抓取地址可以是内网地址；选择直连后，播放器直接访问 M3U 中的频道地址，不经过本服务的 `/live` 或 `/catchup`。

如果需要本服务中转，选择 `mode: proxy`。上游对外输出反代地址、而本服务可以从内网访问同一上游时，可以填写可选的 `upstream_base_url`，将内部请求映射到内网地址。此字段**仅在中转模式生效**；直连模式会忽略它，不会改写下发给播放器的频道或回放地址。中转配置示例：

    name: GITV
    url: http://10.10.10.20:8097/tv.m3u
    mode: proxy
    upstream_base_url: http://10.10.10.20:8097

## 接口

- /list.m3u：聚合订阅
- /admin：源管理，HTTP Basic 鉴权
- /health：不含源 URL 或令牌的状态
- /live/{id}、/catchup/{id}、/proxy/{id}：仅供中转频道使用

此服务使用 host 网络，以便从 Unraid 宿主机的可信地址访问 GITV。服务不管理上游容器，不需要 Docker socket、watchtower 或额外边车。默认监听 28788；可临时修改配置中的 bind，在备用端口验证。
