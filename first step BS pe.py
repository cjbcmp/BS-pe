# -*- coding: utf-8 -*-
import baostock as bs
import pandas as pd
from datetime import datetime, timedelta
import time

def get_all_a_stock_codes():
    """获取全量A股代码（含沪深主板/创业板/科创板/北交所）"""
    rs = bs.query_stock_basic()
    codes = []
    while (rs.error_code == '0') and rs.next():
        code = rs.get_row_data()[0]
        # 匹配所有A股（sh.60, sh.68, sh.688, sz.00, sz.30, sz.002, bj.43, bj.83, bj.87, bj.92）
        if any(code.startswith(prefix) for prefix in [
            'sh.60', 'sh.68', 'sz.00', 'sz.30', 'sz.002', 'sh.688',  # 沪深市场
            'bj.43', 'bj.83', 'bj.87', 'bj.92'  # 北交所新增支持
        ]):
            codes.append(code)
    print(f"Successfully fetched {len(codes)} A-share stock codes (including STAR Market and Beijing Stock Exchange)")
    return codes

def get_real_trade_date():
    """智能获取最近有效交易日（自动校正节假日）"""
    now = datetime.now()
    # 扩展查询范围到30天（覆盖春节等长假）
    rs = bs.query_trade_dates(
        start_date=(now - timedelta(days=30)).strftime('%Y-%m-%d'),
        end_date=now.strftime('%Y-%m-%d'))
    
    trade_dates = []
    while (rs.error_code == '0') and rs.next():
        date_info = rs.get_row_data()
        if date_info[1] == '1':  # 交易日
            trade_dates.append(date_info[0])
    
    if not trade_dates:
        return now.strftime('%Y-%m-%d')
    
    today_str = now.strftime('%Y-%m-%d')
    last_date = trade_dates[-1]
    
    # 核心判断逻辑
    if today_str not in trade_dates:  # 今天非交易日
        print(f"Warning: Current date {today_str} is not a trading day, using last trading day: {last_date}")
        return last_date
    elif now.hour < 15:  # 当天未收盘
        prev_date = trade_dates[-2] if len(trade_dates) >= 2 else last_date
        print(f"Info: Current time {now.strftime('%H:%M')} is before market close, using previous trading day: {prev_date}")
        return prev_date
    else:  # 当天已收盘
        print(f"Successfully using today's closing data: {today_str}")
        return today_str

def fetch_full_a_pe():
    """获取全量A股最新市盈率（含北交所）"""
    print("\n" + "="*50)
    print("  A-share PE Data Collection System (including Beijing Stock Exchange)")
    print("="*50 + "\n")
    
    # 登录
    lg = bs.login()
    if lg.error_code != '0':
        print(f"Error: Login failed: {lg.error_msg}")
        return
    
    # 获取关键参数
    trade_date = get_real_trade_date()
    all_codes = get_all_a_stock_codes()
    
    # 数据存储
    pe_data = {}
    start_time = time.time()
    failed_codes = []
    
    # 批量获取（每600只显示进度）
    for i, code in enumerate(all_codes, 1):
        try:
            rs = bs.query_history_k_data_plus(
                code=code,
                fields="peTTM",
                start_date=trade_date,
                end_date=trade_date,
                frequency="d",
                adjustflag="3")
            
            if rs.data and rs.data[0][0]:
                pe_value = round(float(rs.data[0][0]), 2)
                # 只保留0 < PE ≤ 30的数据
                if 0 < pe_value <= 30:
                    pe_data[code] = pe_value
            
            # 进度显示
            if i % 600 == 0:
                elapsed = time.time() - start_time
                remain = (len(all_codes) - i) * (elapsed / i)
                print(f"Progress: {i}/{len(all_codes)} ({i/len(all_codes):.1%}) | Elapsed: {elapsed:.1f}s | Remaining: {remain:.1f}s")
                
        except Exception as e:
            failed_codes.append(code)
            continue
    
    # 生成DataFrame
    df = pd.DataFrame.from_dict(
        pe_data, 
        orient='index', 
        columns=[f"PE_{trade_date}"]
    )
    df.index.name = "股票代码"
    
    # 按市场分类标记
    
    df['市场'] = df.index.map(lambda x: 
        '沪市' if x.startswith('sh.6') else
        '科创板' if x.startswith('sh.688') else
        '深市' if x.startswith('sz.0') or x.startswith('sz.3') else
        '北交所' if x.startswith('bj.') else '其他'
    )
    
    # 保存文件
    filename = "pe_filtered_stocks_output.xlsx"
    df.to_excel(filename, float_format="%.2f")
    
    # 登出
    bs.logout()
    
    # 结果报告
    print("\n" + "="*50)
    print(f"Data collection complete! Successfully fetched {len(pe_data)} stock data (0 < PE <= 30)")
    print(f"File saved to: {filename}")
    print(f"Total time: {time.time() - start_time:.1f} seconds")
    if failed_codes:
        print(f"Number of failed codes: {len(failed_codes)} (Example: {failed_codes[:5]}...)")
    
    # 按市场统计
    market_stats = df['市场'].value_counts()
    print("\nMarket Statistics:")
    print(market_stats.to_string())
    
    print("="*50 + "\n")
    
    # 数据预览
    print("Data Sample:")
    print(df.sample(5))
    return filename

if __name__ == "__main__":
    fetch_full_a_pe()
