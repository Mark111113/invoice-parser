"""
税务机关查验平台地址映射
根据发票代码前4位确定查验服务器地址
"""

SWJG_MAP = [
    {"code": "1100", "name": "北京", "url": "https://fpcy.beijing.chinatax.gov.cn:443"},
    {"code": "1200", "name": "天津", "url": "https://fpcy.tianjin.chinatax.gov.cn"},
    {"code": "1300", "name": "河北", "url": "https://fpcy.hebei.chinatax.gov.cn"},
    {"code": "1400", "name": "山西", "url": "https://fpcy.shanxi.chinatax.gov.cn:443"},
    {"code": "1500", "name": "内蒙古", "url": "https://fpcy.neimenggu.chinatax.gov.cn:443"},
    {"code": "2100", "name": "辽宁", "url": "https://fpcy.liaoning.chinatax.gov.cn:443"},
    {"code": "2102", "name": "大连", "url": "https://sbf.dalian.chinatax.gov.cn:8402"},
    {"code": "2200", "name": "吉林", "url": "https://fpcy.jilin.chinatax.gov.cn:4432"},
    {"code": "2300", "name": "黑龙江", "url": "https://fpcy.heilongjiang.chinatax.gov.cn:443"},
    {"code": "3100", "name": "上海", "url": "https://fpcy.shanghai.chinatax.gov.cn:1001"},
    {"code": "3200", "name": "江苏", "url": "https://fpcy.jiangsu.chinatax.gov.cn:80"},
    {"code": "3300", "name": "浙江", "url": "https://fpcy.zhejiang.chinatax.gov.cn:443"},
    {"code": "3302", "name": "宁波", "url": "https://fpcy.ningbo.chinatax.gov.cn:443"},
    {"code": "3400", "name": "安徽", "url": "https://fpcy.anhui.chinatax.gov.cn:443"},
    {"code": "3500", "name": "福建", "url": "https://fpcy.fujian.chinatax.gov.cn:443"},
    {"code": "3502", "name": "厦门", "url": "https://fpcy.xiamen.chinatax.gov.cn"},
    {"code": "3600", "name": "江西", "url": "https://fpcy.jiangxi.chinatax.gov.cn:82"},
    {"code": "3700", "name": "山东", "url": "https://fpcy.shandong.chinatax.gov.cn:443"},
    {"code": "3702", "name": "青岛", "url": "https://fpcy.qingdao.chinatax.gov.cn:443"},
    {"code": "4100", "name": "河南", "url": "https://fpcy.henan.chinatax.gov.cn"},
    {"code": "4200", "name": "湖北", "url": "https://fpcy.hubei.chinatax.gov.cn:443"},
    {"code": "4300", "name": "湖南", "url": "https://fpcy.hunan.chinatax.gov.cn:8083"},
    {"code": "4400", "name": "广东", "url": "https://fpcy.guangdong.chinatax.gov.cn:443"},
    {"code": "4403", "name": "深圳", "url": "https://fpcy.shenzhen.chinatax.gov.cn:443"},
    {"code": "4500", "name": "广西", "url": "https://fpcy.guangxi.chinatax.gov.cn:8200"},
    {"code": "4600", "name": "海南", "url": "https://fpcy.hainan.chinatax.gov.cn:443"},
    {"code": "5000", "name": "重庆", "url": "https://fpcy.chongqing.chinatax.gov.cn:80"},
    {"code": "5100", "name": "四川", "url": "https://fpcy.sichuan.chinatax.gov.cn:443"},
    {"code": "5200", "name": "贵州", "url": "https://fpcy.guizhou.chinatax.gov.cn:80"},
    {"code": "5300", "name": "云南", "url": "https://fpcy.yunnan.chinatax.gov.cn:443"},
    {"code": "5400", "name": "西藏", "url": "https://fpcy.xizang.chinatax.gov.cn:81"},
    {"code": "6100", "name": "陕西", "url": "https://fpcy.shaanxi.chinatax.gov.cn:443"},
    {"code": "6200", "name": "甘肃", "url": "https://fpcy.gansu.chinatax.gov.cn:443"},
    {"code": "6300", "name": "青海", "url": "https://fpcy.qinghai.chinatax.gov.cn:443"},
    {"code": "6400", "name": "宁夏", "url": "https://fpcy.ningxia.chinatax.gov.cn:443"},
    {"code": "6500", "name": "新疆", "url": "https://fpcy.xinjiang.chinatax.gov.cn:443"},
]


def get_swjg(fpdm: str) -> dict:
    """
    根据发票代码获取税务机关信息
    
    Args:
        fpdm: 发票代码（10位或12位）
    
    Returns:
        {"name": 省份名, "url": 查验地址, "area": 地区代码}
    """
    # 根据发票代码长度确定地区代码
    if len(fpdm) == 12:
        dqdm = fpdm[1:5]  # 12位发票代码，取第2-5位
    else:
        dqdm = fpdm[0:4]  # 10位发票代码，取前4位
    
    # 特殊城市：大连、宁波、厦门、青岛、深圳
    special_cities = ["2102", "3302", "3502", "3702", "4403"]
    
    if dqdm not in special_cities:
        # 转为省份代码（前2位+00）
        dqdm = dqdm[0:2] + "00"
    
    # 查找对应税务机关
    for swjg in SWJG_MAP:
        if dqdm == swjg["code"]:
            return {
                "name": swjg["name"],
                "url": swjg["url"] + "/NWebQuery",
                "area": dqdm
            }
    
    # 未找到，返回默认
    return {
        "name": "未知",
        "url": "https://inv-veri.chinatax.gov.cn",
        "area": "0000"
    }


def get_invoice_type(fpdm: str) -> str:
    """
    根据发票代码判断发票类型
    
    Args:
        fpdm: 发票代码
    
    Returns:
        发票类型代码
        - 01: 增值税专用发票
        - 02: 货运运输业增值税专用发票
        - 03: 机动车销售统一发票
        - 04: 增值税普通发票（折叠票）
        - 10: 增值税电子普通发票
        - 11: 增值税电子专用发票
    """
    if len(fpdm) == 12:
        # 12位发票代码
        first_digit = fpdm[0]
        code_11_12 = fpdm[10:12]
        
        if first_digit == '0':
            # 增值税发票
            if code_11_12 in ['04', '05', '06', '07', '11', '12']:
                return '04'  # 普票
            elif code_11_12 in ['13']:
                return '01'  # 专票
            elif code_11_12 in ['08']:
                return '10'  # 电子普票
            elif code_11_12 in ['09']:
                return '11'  # 电子专票
        elif first_digit == '1':
            # 机动车发票
            if fpdm[7] == '2':
                return '03'  # 机动车销售统一发票
    
    else:
        # 10位发票代码
        code_8 = fpdm[7]
        if code_8 in ['1', '2', '5', '7']:
            return '01'  # 专票
        elif code_8 in ['3', '6']:
            return '04'  # 票
    
    return '04'  # 默认普票


# 测试
if __name__ == "__main__":
    test_codes = [
        "044001900111",  # 广东普票（12位）
        "011001900411",  # 北京（12位）
        "3300193130",    # 浙江（10位）
        "4400151130",    # 广东（10位）
    ]
    
    for code in test_codes:
        info = get_swjg(code)
        fplx = get_invoice_type(code)
        print(f"发票代码: {code}")
        print(f"  税务机关: {info['name']}")
        print(f"  查验地址: {info['url']}")
        print(f"  地区代码: {info['area']}")
        print(f"  发票类型: {fplx}")
        print()