# 更新日志总结

- **日志改进**：更新了 `machine_lib.py`，将日志时间改为使用 `logging` 库来记录，增强了日志的管理和格式化功能。
  
- **健壮性增强**：`check.py` 进行了改进，增强了对新规则的适应性，确保在运行过程中更稳定。

- **依赖更新**：在 `requirements.txt` 中加入了 `loguru` 库，用于更好地支持日志记录。

- **因子状态追踪**：`digging_1step.py` 和 `digging_2step.py` 进行了更新，允许在重新运行时知道已经跑过多少个因子。

- **新增功能**：`check.py` 增加了微信提醒功能，可以自动发送因子提交的通知（需要配置 `sever酱` 的 `secret key`）。

- **数据增强**：在 `fields.py` 中增加了豆包推荐数据，用于丰富模型的输入。

- **SSL 问题修复**：在 `machine_lib.py` 中增加了 `conn = aiohttp.TCPConnector(ssl=False)`，解决了 SSL 连接问题。

- **初始功能**：项目的初始版本包括了因子挖掘、检查和提交的基本功能，支持自动化因子提交。

---

## 使用说明

1. python3
2. 配置好 python 环境
3. 配置 [user_info.txt](user_info.txt) 文件
4. 运行 [digging_1step.py](digging_1step.py) 进行第一轮挖掘，注意配置好 step1_tag
5. 运行 [digging_2step.py](digging_2step.py) 进行第二轮挖掘，注意要和 step1_tag 一致，然后修改好 step2_tag
6. `dataset_id = 'analyst4'`，这个 id 可以在世坤平台的 data 中找到，在 URL 里。例如：[https://platform.worldquantbrain.com/data/data-sets/analyst4?delay=1&instrumentType=EQUITY&limit=20&offset=0&region=USA&universe=TOP3000](https://platform.worldquantbrain.com/data/data-sets/analyst4?delay=1&instrumentType=EQUITY&limit=20&offset=0&region=USA&universe=TOP3000)
7. 运行 [check.py](check.py)，全自动获取可以提交的因子，如果有可以提交的因子会在 [records](records) 文件夹下产生 `submitable_alpha.csv` 文件
8. 找到 `submitable_alpha.csv` 文件中可以提交的因子的 id，修改 [submit_alpha.py](submit_alpha.py) 并且运行，可以自动提交因子
