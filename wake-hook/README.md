# 给新机器接入 AgentLink —— 极简三步

目标：让另一台 Windows 机器接入网格，**收发消息 + 收到消息自动唤醒 AI**。

前提条件（唯一）：新机器装了 **WSL + Hermes Agent**，D 盘 Windows 路径 WSL 能访问。

---

## 第 1 步：把安装包拷到新机器

从本机 `D:\HermesSpace\AgentLink\dist\` 拷这两个文件（U盘/微信/任何方式）：

    AgentLink-v0.4.10-bundle.zip    ← 节点程序（内含自启脚本、注册技能、唤醒工具包）
    启动.bat                         ← 启动器

在新机器上同样放到 `D:\HermesSpace\AgentLink\dist\`（路径要一致），双击 `启动.bat`。
→ 看到"已启动"后，开机自启、防火墙、目录准备全部自动搞定，节点 8765 开始监听。
→ 想让别的机器发现它：编辑 `agentlink.toml`，把 `peers` 加上已有机器（如 `192.168.1.7:8765`），重开启动.bat。

## 第 2 步：接"消息自动唤醒 AI"的线

新机器 WSL 里执行（先 `unzip` 解包 bundle）：

    cd dist/AgentLink-v0.4.10-bundle/wake-kit
    bash install-all.sh 你机器上的profile名

全自动完成，中途要装依赖会提示输一次 sudo 密码。跑完最后显示"完成 ✅"即成功。

## 第 3 步：给它注册 AI 身份

在新机器上打开它的 Hermes（`hermes -p <profile>`），对它说：

    读取 D:\HermesSpace\AgentLink\dist\AgentLink-v0.4.10-bundle\SKILL.md，
    按其注册 AgentLink 身份，client 名用 <profile 同名>。

bundle 根目录的 SKILL.md 就是注册技能，AI 会自己完成：节点注册 → 建 agents 工作区 →
学会收发消息。桥（第 2 步已起）会在重连时自动带上注册（内置 register_all）。

## 验证（回本机 50 做）

    TOKEN=$(grep '^token' /mnt/c/Users/yxf/AppData/Local/AgentLink/data/agents/fleet/study.toml | cut -d'"' -f2)
    curl -s --noproxy '*' -X POST http://127.0.0.1:8765/v1/send \
      -H "Authorization: Bearer $TOKEN" -H 'Content-Type: application/json' \
      -d '{"topic":"bus.chat","to":"新agent_id","to_agent":"新名字","content":{"type":"text","text":"网格问候"},"from":"w50-selftest","from_agent":"study"}'

新机器的 `D:\HermesSpace\.fleet-<node_id>\agents\inbox\` 落文件 = 收信链路通；
过几分钟去那台机器 `hermes -p <profile> sessions list` 应看到新活动 = 自动唤醒也通。

---

**出错了？** 把命令输出原样发给 50 号机的 study 会话，它会接手诊断。
