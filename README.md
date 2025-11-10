# Spring Reactive Demo 项目

## 项目简介
这是一个基于Spring Boot Reactive的演示项目，包含JWT认证、MySQL数据库和Redis缓存功能。

## Docker 部署

### 前置条件
- 已安装 Docker 和 Docker Compose
- 确保 8080、3306 和 6379 端口未被占用
# docker build -t spring-reactive-test . 
# 连接到本地Redis和RabbitMQ服务的完整命令（使用--network=host或明确指定网络）
<!-- docker run -d --name spring-reactive-test-1 \
  -p 8081:8080 \
  --network=my-network \
  spring-reactive-test -->
  <!-- docker run -d \
  --name jenkins \
  -p 8082:8080 \
  -p 50002:50000 \
  -v /opt/jenkins_home:/var/jenkins_home \
  jenkins/jenkins -->
### 快速启动
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
1. 克隆项目到本地
```bash
git clone <项目地址>
cd spring-reactive-demo
```

2. 使用 Docker Compose 启动所有服务
```bash
docker-compose up -d
```

这将会：
- 构建应用镜像
- 启动 MySQL 数据库（端口 3306）
- 启动 Redis 缓存（端口 6379）
- 启动应用服务（端口 8080）

### 环境配置

所有环境变量都在 `.env` 文件中定义，可以根据需要修改：
- 数据库配置
- Redis 密码
- JWT 密钥
- 应用端口

### 构建单独的应用镜像

```bash
docker build -t spring-reactive-app .
```

### 查看服务日志

```bash
docker-compose logs -f
```

### 停止服务

```bash
docker-compose down
```

### 停止服务并删除数据卷

```bash
docker-compose down -v
```

## 注意事项

1. 首次启动时，MySQL 会自动执行 `schema.sql` 初始化数据库
2. 确保 `.env` 文件中的密码等敏感信息妥善保管
3. 生产环境部署前建议修改默认的密码和密钥