from flask import Flask, render_template, request
import pandas as pd
import pymssql

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


