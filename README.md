# kafka-pressure-test

kafka 压力测试项目

## 使用示例

#### 发送数据

1. node 直接启动

```bash
$ node ../test/lib/sendMessage.js
```

2. vscode 调试方式启动

### 消费数据

1. 单进程

```bash
$ npm run dev
```

2. 多进程(进程数量可通过 workers 数量修改)

```bash
$ npm start
```
