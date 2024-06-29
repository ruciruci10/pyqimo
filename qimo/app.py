from flask import Flask, render_template, request
import pandas as pd
import pymssql
import requests
from bs4 import BeautifulSoup
import csv

def get_page_data(page_num, csvwriter):
    url = f'https://fangjia.gotohui.com/fjdata-{page_num}'
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36'
    }
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        soup = BeautifulSoup(response.content, 'html.parser')
        city_name_tag = soup.find('a', class_='name')
        city_name = city_name_tag.text.strip() if city_name_tag else "未知城市"
        title_tag = soup.find('th', string="近十年房价极差")
        if title_tag:
            max_price = title_tag.find_next('span', class_='value red').text.strip()
            min_price = title_tag.find_next('span', class_='value green').text.strip()
            csvwriter.writerow([city_name, max_price, min_price])
        else:
            csvwriter.writerow([city_name, "无", "无"])
    except requests.exceptions.RequestException:
        print(f"获取第 {page_num} 页数据时出错")

with open('pachong1.csv', 'w', newline='', encoding='utf-8') as csvfile:
    csvwriter = csv.writer(csvfile)
    csvwriter.writerow(['城市', '最高价', '最低价'])

    for page_num in range(1, 301):
        get_page_data(page_num, csvwriter)

print("数据已保存到 pachong1.csv 文件中")

# 读取CSV文件
df = pd.read_csv('pachong1.csv', encoding='utf-8')

# 连接到SQL Server数据库
conn = pymssql.connect(server='127.0.0.1', user='sa', password='123456', database='master')

# 创建数据库游标
cursor = conn.cursor()

# 将数据加载到SQL Server中
for index, row in df.iterrows():
    city = str(row['城市'])
    # 检查最高价和最低价是否为有效浮点数
    try:
        max_price = float(row['最高价'])
    except ValueError:
        max_price = None  # 如果无法转换为浮点数，则将其设置为None或者其他默认值
    try:
        min_price = float(row['最低价'])
    except ValueError:
        min_price = None  # 如果无法转换为浮点数，则将其设置为None或者其他默认值

    cursor.execute("INSERT INTO pachong1(City, MaxPrice, MinPrice) VALUES(%s, %s, %s)",
                   (city, max_price, min_price))

# 提交更改
conn.commit()
# 关闭数据库连接
conn.close()

def get_page_data(page_num, writer):
    url = f'https://fangjia.gotohui.com/fjdata-{page_num}'
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/94.0.4606.81 Safari/537.36'
    }

    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        soup = BeautifulSoup(response.content, 'html.parser')
        page_content = soup.find(class_="ntable table-striped table-hover")
        city_name_tag = soup.find('a', class_='name')
        if page_content and city_name_tag:
            city = city_name_tag.text.strip()
            rows = page_content.find_all('tr')
            for row in rows[1:]:
                columns = row.find_all('td')
                if len(columns) > 1:
                    price = columns[2].text.strip()
                else:
                    price = "无数据"
                writer.writerow([city, price])
    except requests.exceptions.RequestException:
        print(f"获取第 {page_num} 页数据时出错")

with open('pachong2.csv', 'w', newline='', encoding='utf-8') as csvfile:
    csvwriter = csv.writer(csvfile)
    csvwriter.writerow(['城市', '价格'])
    for page_num in range(1, 301):
        get_page_data(page_num, csvwriter)

print("数据已保存到 pachong2.csv 文件中")

# 读取 CSV 文件
df = pd.read_csv('pachong2.csv', encoding='utf-8')

# 处理缺失值和非法值
df['价格'] = pd.to_numeric(df['价格'], errors='coerce')  # 将价格列转换为数值类型并处理非法值为 NaN

# 连接到 SQL Server 数据库
conn = pymssql.connect(server='127.0.0.1', user='sa', password='123456', database='master')

# 创建数据库游标
cursor = conn.cursor()

# 插入数据到数据库
for index, row in df.iterrows():
    city = str(row['城市'])
    price = row['价格'] if not pd.isna(row['价格']) else None  # 处理 NaN 值
    cursor.execute("INSERT INTO pachong2(城市, 价格) VALUES(%s, %s)", (city, price))

conn.commit()  # 提交事务
conn.close()  # 关闭数据库连接

url = "https://fangjia.gotohui.com/"
headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36'
}

res = requests.get(url, headers=headers)
res.encoding = 'utf-8'
html = res.text
soup = BeautifulSoup(html, 'html.parser')
page_content = soup.find(class_="ntable table-striped table-hover")
rows = page_content.find_all('tr')

# 准备数据列表以存储字典
data = []

for row in rows[1:]:
    columns = row.find_all('td')
    rank = columns[0].text.strip()
    city = columns[1].text.strip()
    second_hand_price = columns[2].text.strip()
    year_on_year = columns[3].text.strip()
    month_on_month = columns[4].text.strip()
    new_house_price = columns[5].text.strip()

    # 为每一行数据创建一个字典
    data.append({
        "排名": rank,
        "城市": city,
        "二手房价格": second_hand_price,
        "同比": year_on_year,
        "环比": month_on_month,
        "新房价格": new_house_price
    })

csv_file = 'pachong3.csv'
fields = ["排名", "城市", "二手房价格", "同比", "环比", "新房价格"]
with open(csv_file, mode='w', newline='', encoding='utf-8') as file:
    writer = csv.DictWriter(file, fieldnames=fields)

    # 写入字段名
    writer.writeheader()

    # 写入数据
    for row in data:
        writer.writerow(row)

print(f"成功将数据写入CSV文件：{csv_file}")


# 连接到 SQL Server 数据库
conn = pymssql.connect(server='127.0.0.1', user='sa', password='123456', database='master')

# 创建数据库游标
cursor = conn.cursor()

# 读取 CSV 文件
df = pd.read_csv('pachong3.csv', encoding='utf-8')

# 插入数据到数据库
for index, row in df.iterrows():
    rank = str(row['排名'])
    city = str(row['城市'])
    second_hand_price = str(row['二手房价格'])
    year_on_year = str(row['同比'])
    month_on_month = str(row['环比'])
    new_house_price = str(row['新房价格'])
    cursor.execute("INSERT INTO pachong3(排名, 城市, 二手房价格, 同比, 环比, 新房价格) VALUES(%s, %s, %s, %s, %s, %s)",
                   (rank, city, second_hand_price, year_on_year, month_on_month, new_house_price))

conn.commit()  # 提交事务
conn.close()  # 关闭数据库连接

app = Flask(__name__)

def get_data_from_database():
    try:
        conn = pymssql.connect(server='127.0.0.1', user='sa', password='123456', database='master', charset='cp936')
        cursor = conn.cursor()

        cursor.execute("SELECT * FROM pachong3")

        data = cursor.fetchall()
        column_names = ['排名', '城市', '二手房价格', '同比', '环比', '新房价格']
        df = pd.DataFrame(data, columns=column_names)
        conn.close()
        return df

    except Exception as e:
        print(f"Error fetching data from database: {str(e)}")
        return pd.DataFrame()

@app.route('/', methods=['GET', 'POST'])
def display_table():
    data = get_data_from_database()

    if request.method == 'POST':
        city_name = request.form['city'].strip()
        if city_name:
            # Filter data based on the entered city name
            data = data[data['城市'].str.contains(city_name)]

    table = data.to_html(index=False, header=True)

    return render_template('table.html', table=table)

if __name__ == '__main__':
    app.run(debug=True)


