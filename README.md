# 全链路压测 · 流量隔离中间层(仿真)

在生产级仿真电商链路上实现的全链路压测流量隔离方案。纯 Go 标准库,无外部依赖。

## 链路

```
client
  └─ gateway(入口染色、熔断检查)
      └─ order(下单编排)
          ├─ user        同步 · 缓存+库
          ├─ risk        同步 · 缓存
          ├─ coupon      同步 · 库
          ├─ inventory   同步 · 库+缓存
          ├─ payment     同步 · 缓存幂等 + 第三方支付挡板 + 库
          │    └─ notification  同步 · 短信挡板 + 库
          ├─ 线程池异步任务:写审计日志(上下文自动透传)
          └─ MQ: order_created
               ├─ notification 消费者  短信挡板 + 库
               └─ logistics    消费者  物流挡板 + 库
定时任务(order):超时关单,扫真实表 → MQ: order_timeout → notification 消费者
```

数据库、缓存、消息队列、第三方接口均为进程内仿真(`internal/infra`),真实/影子两套并存,便于验收零污染。

## 中间层能力

1. **流量染色与透传**(`internal/middleware`)
   - 入口按 `X-Stress-Flag` 染色,标记放进 `context.Context`;
   - 同步调用:`middleware.Do` 自动注入请求头,`HTTPMiddleware` 自动恢复;
   - 消息队列:`MQProxy.Produce` 写入消息头,消费影子主题时自动恢复上下文;
   - 线程池:`Executor.Submit` 提交时捕获上下文,工作线程执行时恢复;
   - 业务代码全程不手动传递标记。
2. **影子库表路由**(`infra.DBProxy`):带标记读写自动路由到 `shadow_` 前缀表;影子表不存在**拒绝写入**,绝不回退真实表。
3. **缓存/消息隔离**(`infra.CacheProxy` / `infra.MQProxy`):压测缓存键加 `stress__` 前缀;压测消息进入 `__stress` 影子主题。
4. **外部调用挡板**(`infra.Guard`):带标记的短信/支付/物流调用返回 `MOCK_OK`,不真正发出。
5. **漏标检测与熔断**(`middleware.M`):
   - 每个服务入口统计压测流量(按压测用户号段 9xxxxxx 识别)的标记丢失比例;
   - 库/缓存/MQ/第三方各层做数据特征校验,发现压测数据进入真实环境立即告警并熔断;
   - 熔断后网关在入口直接拒绝压测流量(503)。

## 运行

```bash
# 启动仿真链路 + 监控大盘(http://127.0.0.1:19000)
go run ./cmd/server

# 人为让 payment 不接入中间层,演示漏标检测与熔断
go run ./cmd/server -rogue=payment

# 发流量
curl -d '{"user_id":1001,"sku_id":"sku-1","qty":1}' localhost:18001/api/order                      # 正常
curl -H 'X-Stress-Flag: 1' -d '{"user_id":9000001,"sku_id":"sku-1","qty":1}' localhost:18001/api/order  # 压测

# 一键验收
go run ./cmd/acceptance
```

## 验收内容(`cmd/acceptance`)

- **阶段 A**(全部接入):混合跑 40 正常 + 40 压测订单,检查
  1. 真实库表/真实缓存/真实主题/真实第三方调用记录中压测数据 = 0;
  2. 同步、MQ、线程池三种调用方式的标记透传成功率 = 100%;
  3. 影子表写入统计、缓存前缀、影子主题、第三方挡板生效;影子表缺失时拒绝写入且不回退;
  4. 中间层各组件额外延迟(平均/P99/最大)。
- **阶段 B**(payment 不接入):发压测流量,测量从进入到熔断的检测耗时(实测约 2ms),验证熔断后入口 503、告警明细。

监控大盘(`:19000`)展示:链路拓扑上每个服务的压测流量与透传率、影子表/真实表行数、缓存键与主题隔离情况、挡板拦截统计、中间层延迟、漏标告警与熔断状态。
