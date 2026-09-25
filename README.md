# home-iptv-proxy

独立的 M3U 源管理服务。启动时源列表为空；在后台逐条添加源，并为每条源明确选择：

- **直连**：聚合列表保留上游频道播放地址和回放属性。适合 4GTV 这类播放器可直接访问的源。
- **中转**：聚合列表使用本地固定直播地址；如果上游频道带有回放地址，则输出 APTV 可用的本地回放地址。适合 GITV 这类需要由本服务转发播放请求的源。

中转在播放器请求频道或回放时才访问上游。HLS 的子清单、分片、密钥以及 Range 请求会继续中转。频道没有上游回放属性时，不会虚构回放能力。服务不会代替 GITV 或 4GTV 自身的登录、播放鉴权与续期；这些仍由各自的上游服务处理。

## 部署

1. 复制 .env.example 为 .env，设置强密码 IPTV_ADMIN_PASSWORD。
2. 将 config/sources.yaml 的 signing_secret 改为至少 32 字符的随机值。首次启动保持空源列表。
3. 运行 docker compose up -d --build。

默认后台：http://主机地址:28788/admin，用户名 admin，密码为 .env 中的值。

固定订阅：http://主机地址:28788/list.m3u。

后台支持添加、编辑、删除、启停源以及手动刷新，服务还会按 refresh_minutes 定时刷新。每条源都必须指定播放方式。源配置保存在 config/sources.yaml；更新镜像时挂载目录会保留配置。

GITV 源可添加其现有订阅接口并选择“中转”。服务保留上游 M3U 的 x-tvg-url，把频道级 catchup-source 转成 APTV 的 catchup=default 和本地时间戳回放入口。4GTV 源选择“直连”，下发它原来的频道地址。

如果源对外输出的是反代地址，但服务所在主机可以通过内网直接访问同一上游，可以为该源填写可选的 upstream_base_url。服务只在内部中转时把频道、HLS 子清单、分片和密钥映射到这个地址；源本身对外输出的地址不会被修改。GITV 的典型配置是：

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
