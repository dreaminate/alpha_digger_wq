

1. python3
2. 配置好python环境
3. 配置[user_info.txt](user_info.txt)文件
4. 运行[digging_1step.py](digging_1step.py)进行第一轮挖掘，注意配置好step1_tag
5. 运行[digging_2step.py](digging_2step.py)进行第二轮挖掘，注意要和step1_tag一致，然后修改好step2_tag
6. dataset_id = 'analyst4'，这个id可以在世坤平台的data中找到，在url里
例如https://platform.worldquantbrain.com/data/data-sets/analyst4?delay=1&instrumentType=EQUITY&limit=20&offset=0&region=USA&universe=TOP3000
1. 运行[check.py](check.py)，全自动获取可以提交的因子，如果有可以提交的因子会在[records](records)文件夹下产生submitable_alpha.csv文件
2. 找到submitable_alpha.csv文件中可以提交的因子的id，修改[submit_alpha.py](submit_alpha.py)并且运行，可以自动提交因子
    

"""
更新日志

v1.1.8 2025.09.11
1. [machine_lib.py](machine_lib.py) 官方API有改动，修复了duplicate的问题

v1.1.7 2025.05.15
1. [machine_lib.py](machine_lib.py) 增加登陆失败的报错，解决None的Session问题

v1.1.6 2025.05.10
1. [machine_lib.py](machine_lib.py) 适配了digging，logging增加时间
2. digging_1step.py 更加稳健，适应新规则
3. digging_2step.py 更加稳健，适应新规则
4. config.py 删除没必要的region

v1.1.5 2025.05.09
1. [machine_lib.py](machine_lib.py) 日志时间改成了logging
2. check.py 更加稳健，适应新规则

v1.1.4 2025.04.08
1. [requirements.txt](requirements.txt) 多了loguru库
2. [machine_lib.py](machine_lib.py) 支持日志时间
3. [digging_1step.py](digging_1step.py), [digging_2step.py](digging_2step.py) 重新运行时可以知道已经跑了多少个因子

v1.1.3 2025.03.19
1. check.py 增加微信提醒功能，需要自己获取sever酱的secret key替换

v1.1.2 2025.03.04
1. fields.py 增加豆包推荐数据

v1.1.1 2025.02.24
1. machine_lib.py  增加：conn = aiohttp.TCPConnector(ssl=False) 解决ssl问题

v1.0 2025.01.02
初始版本
功能：
1. digging_1step.py 第一轮挖掘
2. digging_2step.py 第二轮挖掘
3. check.py自动获取可以提交的因子
4. submit_alpha.py自动提交因子

"""


