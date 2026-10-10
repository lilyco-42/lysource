# lysource — 资源爬取与整合器

> lyco 生态 · 可靠信息获取源 + 自动化请求服务。单文件核心（~300 行），SQLite 存储，单容器部署。

## 架构（原子化）

```
sources.yaml / POST /v1/sources     # 信息源配置（rss / html选择器）
        ↓
APScheduler 心跳（60s）→ 抓取到期源 → 清洗去重 → SQLite items
        ↓                                    ↓
GET /v1/resources?q=                可信度×时间衰减评分（trust·e^(-h/72)）
POST /v1/fetch {url, selector?}     即时代理抓取（10min 缓存）
```

## API

| 方法 | 路径 | 说明 | 鉴权 |
|------|------|------|------|
| GET  | `/healthz` | 健康检查 | 无 |
| GET  | `/v1/sources` | 源列表+抓取状态 | Token |
| POST | `/v1/sources` | 添加源（新增立即抓） | Token |
| GET  | `/v1/resources?q=&limit=` | 整合查询，按可靠性评分排序 | Token |
| POST | `/v1/fetch` `{url, selector?}` | 自动化请求服务（即时抓取代理） | Token |
| POST | `/v1/refresh` | 手动触发全量抓取 | Token |

鉴权：请求头 `X-API-Token: <LYSOURCE_TOKEN>`；未设置环境变量时开放（仅限本地）。

## 本地跑

```bash
pip install -r requirements.txt
uvicorn app:app --port 8790   # 打开 http://127.0.0.1:8790/docs
```

## 部署到 lain42.top

```bash
LYSOURCE_TOKEN=换成长随机串 docker compose up -d -b
```

容器只监听 `127.0.0.1:8790`，在 pingap 加一条反代注册为子服务：

```toml
[servers.lysource]
listen = "0.0.0.0:443"
[servers.lysource.routes.res]         # res.lain42.top → lysource
upstream = "127.0.0.1:8790"
```

（或直接用你现有的 pingap 配置模板加 upstream `127.0.0.1:8790`，路径 `/lysource/` 亦可。）

## 扩展点（后续模块化）

- `type=json` 源（官方 API 直采）· 上游挂 RSSHub 46k⭐ 扩路 · Webhook 推送
- 抓取失败的源自动降 trust · 多实例共享 DB
