# qqbot-24h

把这个 QQ 官方机器人跑在 GitHub Actions 上，实现不需要自己电脑开机、零成本的 7×24 在线。

- 接的是 QQ 开放平台官方机器人（WebSocket 长连接，不需要公网 IP、不需要挂 QQ 客户端）
- 模型用智谱 GLM-4.7-Flash（官方免费模型），也支持任何 OpenAI 兼容接口
- 单次运行 5 小时 45 分，结束前自动触发下一次，形成无缝接力
- 聊天记忆通过 actions/cache 在多次运行间保留
- `concurrency` + 启动前实例检查，保证同一时间只有一个实例连 QQ

## 需要的 Secrets

| 名称 | 说明 |
|---|---|
| `QQ_APP_ID` | QQ 开放平台 AppID |
| `QQ_APP_SECRET` | QQ 开放平台 AppSecret |
| `LLM_API_KEY` | 大模型 API Key（智谱 open.bigmodel.cn） |
| `LLM_BASE_URL` | 默认 `https://open.bigmodel.cn/api/paas/v4` |
| `LLM_MODEL` | 默认 `glm-4.7-flash` |
| `GH_PAT` | 用于触发接力运行的 GitHub Token（需 `repo`+`workflow`） |
| `SYSTEM_PROMPT` | 可选，机器人人设 |

## 说明

- 仓库设为 public 是因为公开仓库的 Actions 免费且不限分钟；私有仓库每月只有 2000 分钟，撑不住 24×7。
- 密钥都存在 GitHub Secrets 里（加密存储），不会出现在代码中。
