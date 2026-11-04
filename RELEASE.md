# 发布说明（GitHub 源码 + 容器镜像）

## 一、发布源码（更新版本）

```bash
# 在仓库根目录
git add -A
git commit -m "release: v1.0.0"
git tag -a v1.0.0 -m "v1.0.0"
git push origin main
git push origin v1.0.0
```

随后在 GitHub 仓库 → Releases → Draft a new release → 选择 `v1.0.0` 标签，附上 CHANGELOG 内容即可。

## 二、发布容器镜像（ghcr.io）

### 方式 A：GitHub Actions 自动构建（推荐）

仓库已附带工作流模板 `deploy/github-actions-docker-publish.yml`（放在 `deploy/` 而非 `.github/workflows/`，避免推送时因 token 缺少 `workflow` scope 被拒）：

1. 在 GitHub 网页上新建文件 `.github/workflows/docker-publish.yml`，内容直接粘贴 `deploy/github-actions-docker-publish.yml`；
   - 或用带 `workflow` scope 的 PAT 在本地执行：
     `mkdir -p .github/workflows && cp deploy/github-actions-docker-publish.yml .github/workflows/docker-publish.yml && git add . && git commit -m "ci: add docker publish" && git push`
2. 打 `v*` 标签推送，或在 Actions 页面手动 `Run workflow`；
3. 构建完成后镜像地址：

```
ghcr.io/<owner>/<repo>:latest
ghcr.io/<owner>/<repo>:1.0.0
ghcr.io/<owner>/<repo>:2.3
ghcr.io/<owner>/<repo>:<short-sha>
```

认证使用内置 `GITHUB_TOKEN`，无需额外配置 Secret。
注意：首次推送后镜像默认私有，需在仓库 → Packages → 该镜像 → Package settings → Change visibility 改为 Public，否则他人 `docker pull` 会 404。

### 方式 B：本地构建并推送

```bash
# 1. 登录（密码处填 GitHub PAT，需 write:packages 权限）
echo "<PAT>" | docker login ghcr.io -u <github-user> --password-stdin

# 2. 构建（需能访问 Docker Hub；国内可先配置镜像加速）
docker build -t ghcr.io/<owner>/<repo>:1.0.0 -t ghcr.io/<owner>/<repo>:latest .

# 3. 推送
docker push ghcr.io/<owner>/<repo>:1.0.0
docker push ghcr.io/<owner>/<repo>:latest
```

镜像约 4–6 GB（含 CUDA 运行时与 faster-whisper 依赖），推送耗时取决于上行带宽；建议优先用方式 A。

### 用户侧使用

```yaml
services:
  subtitle-forge:
    image: ghcr.io/<owner>/<repo>:latest
    # ... 其余同 docker-compose.yml
```

```bash
docker compose pull && docker compose up -d
```

## 三、发布前检查清单

- [ ] `config/`、`data/`、`logs/`、`models/` 未入库（.gitignore 已覆盖）
- [ ] `.env` 未入库，仓库只保留 `.env.example`
- [ ] `docker-compose.yml` 中宿主路径已替换为示例路径
- [ ] 无 API Key / Token / 内网地址 / 个人账号残留
- [ ] `app/` 下无 `*.bak`、`*_2026*.py`、`__pycache__`
