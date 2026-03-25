---
name: 保持 Exception 类型
description: _verify_deploy_tables 抛出 Exception 而非 RuntimeError，测试应匹配 Exception
type: feedback
---

`_verify_deploy_tables` 中故意抛出 `Exception`，不改为 `RuntimeError`。

**Why:** 调用方只做 `sys.exit(1)`，不需要区分异常类型，保持 `Exception` 足够。

**How to apply:** 遇到此处异常类型不匹配时，改测试来匹配 `Exception`，不改代码。
