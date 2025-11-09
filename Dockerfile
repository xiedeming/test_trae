# 第一阶段：构建应用
FROM maven:3.8.4-openjdk-8-slim AS builder

# 设置工作目录
WORKDIR /app

# 添加Maven配置，使用多个可靠的国内镜像源
RUN mkdir -p /root/.m2 && \
    echo '<settings xmlns="http://maven.apache.org/SETTINGS/1.0.0"' > /root/.m2/settings.xml && \
    echo '          xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"' >> /root/.m2/settings.xml && \
    echo '          xsi:schemaLocation="http://maven.apache.org/SETTINGS/1.0.0 http://maven.apache.org/xsd/settings-1.0.0.xsd">' >> /root/.m2/settings.xml && \
    echo '  <mirrors>' >> /root/.m2/settings.xml && \
    echo '    <!-- 华为云Maven仓库 -->' >> /root/.m2/settings.xml && \
    echo '    <mirror>' >> /root/.m2/settings.xml && \
    echo '      <id>huaweicloud</id>' >> /root/.m2/settings.xml && \
    echo '      <name>Huawei Cloud Repository</name>' >> /root/.m2/settings.xml && \
    echo '      <url>https://repo.huaweicloud.com/repository/maven/</url>' >> /root/.m2/settings.xml && \
    echo '      <mirrorOf>*</mirrorOf>' >> /root/.m2/settings.xml && \
    echo '    </mirror>' >> /root/.m2/settings.xml && \
    echo '    <!-- 阿里云Maven仓库 -->' >> /root/.m2/settings.xml && \
    echo '    <mirror>' >> /root/.m2/settings.xml && \
    echo '      <id>aliyunmaven</id>' >> /root/.m2/settings.xml && \
    echo '      <name>Aliyun Maven Repository</name>' >> /root/.m2/settings.xml && \
    echo '      <url>https://repo1.maven.org/maven2/</url>' >> /root/.m2/settings.xml && \
    echo '      <mirrorOf>central</mirrorOf>' >> /root/.m2/settings.xml && \
    echo '    </mirror>' >> /root/.m2/settings.xml && \
    echo '  </mirrors>' >> /root/.m2/settings.xml && \
    echo '  <proxies>' >> /root/.m2/settings.xml && \
    echo '    <!-- 可选：如果需要代理，取消注释并修改下面的配置 -->' >> /root/.m2/settings.xml && \
    echo '    <!--' >> /root/.m2/settings.xml && \
    echo '    <proxy>' >> /root/.m2/settings.xml && \
    echo '      <id>proxy</id>' >> /root/.m2/settings.xml && \
    echo '      <active>true</active>' >> /root/.m2/settings.xml && \
    echo '      <protocol>http</protocol>' >> /root/.m2/settings.xml && \
    echo '      <host>proxy.example.com</host>' >> /root/.m2/settings.xml && \
    echo '      <port>8080</port>' >> /root/.m2/settings.xml && \
    echo '    </proxy>' >> /root/.m2/settings.xml && \
    echo '    -->' >> /root/.m2/settings.xml && \
    echo '  </proxies>' >> /root/.m2/settings.xml && \
    echo '</settings>' >> /root/.m2/settings.xml

# 先复制pom.xml并下载依赖（利用Docker层缓存）
COPY pom.xml .
RUN mvn dependency:go-offline -B -U --no-transfer-progress

# 然后再复制源代码并构建
COPY src ./src
RUN mvn clean package -DskipTests

# 第二阶段：运行应用
FROM openjdk:8-jre-slim

# 设置工作目录
WORKDIR /app

# 从构建阶段复制构建产物
COPY --from=builder /app/target/spring-reactive-demo-0.0.1-SNAPSHOT.jar app.jar

# 设置时区
RUN ln -sf /usr/share/zoneinfo/Asia/Shanghai /etc/localtime && echo 'Asia/Shanghai' > /etc/timezone

# 设置环境变量
ENV JAVA_OPTS="-Xmx1024m -Xms256m"

# 暴露应用端口
EXPOSE 8080

# 运行应用，使用test配置文件
ENTRYPOINT ["sh", "-c", "java $JAVA_OPTS -jar app.jar --spring.profiles.active=test"]