from datetime import datetime, timedelta
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import pandahouse as ph
import io
from airflow.decorators import dag, task

connection = {
    'host': 'http://clickhouse.lab.karpov.courses:8123',
    'password': 'dpo_python_2020',
    'user': 'student',
    'database': 'simulator_20260520'
}

default_args = {
    'owner': 'i_maksutov',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2026, 7, 26)
}

def check_anomaly(df, metric, threshold=0.3):
    current_ts = df['ts'].max()
    day_ago_ts = current_ts - pd.DateOffset(days=1)

    current_value = df[df['ts'] == current_ts][metric].iloc[0]
    day_ago_value = df[df['ts'] == day_ago_ts][metric].iloc[0]

    if current_value <= day_ago_value:
        diff = abs(current_value / day_ago_value - 1)
    else:
        diff = abs(day_ago_value / current_value - 1)

    is_alert = 1 if diff > threshold else 0
    return is_alert, current_value, diff

def send_alert(metric, current_value, diff, df):
    
    dashboard_url = "https://superset.lab.karpov.courses/superset/dashboard/8932/"
    
    msg = f"""
🚨 АЛЕРТ!

Метрика: {metric}
Текущее значение: {current_value:.2f}
Отклонение от вчера: {diff:.2%}
Время: {datetime.now().strftime('%Y-%m-%d %H:%M')}

📊 Дашборд: {dashboard_url}
    """
    print(msg)

    sns.set(rc={'figure.figsize': (16, 10)})
    plt.tight_layout()

    ax = sns.lineplot(
        data=df.sort_values(by=['date', 'hm']),
        x='hm',
        y=metric,
        hue='date'
    )

    for ind, label in enumerate(ax.get_xticklabels()):
        if ind % 15 == 0:
            label.set_visible(True)
        else:
            label.set_visible(False)

    ax.set_xlabel('Время')
    ax.set_ylabel(metric)
    ax.set_title(f'Метрика: {metric}')
    ax.set(ylim=(0, None))

    plot_object = io.BytesIO()
    ax.figure.savefig(plot_object)
    plot_object.seek(0)
    plot_object.name = f'{metric}.png'
    plt.close()

    print(f"График сохранён: {metric}.png")

@dag(default_args=default_args,
     schedule_interval='*/15 * * * *',
     catchup=False)
def dag_alerts_ilgam():

    @task()
    def check_feed():
        q = """
        SELECT
            toStartOfFifteenMinutes(time) as ts,
            toDate(ts) as date,
            formatDateTime(ts, '%R') as hm,
            uniqExact(user_id) as users_feed,
            countIf(action = 'view') as views,
            countIf(action = 'like') as likes,
            round(countIf(action = 'like') /
                  countIf(action = 'view'), 4) as CTR
        FROM simulator_20260520.feed_actions
        WHERE ts >= today() - 1
            AND ts < toStartOfFifteenMinutes(now())
        GROUP BY ts, date, hm
        ORDER BY ts
        """
        df = ph.read_clickhouse(q, connection=connection)

        metrics = ['users_feed', 'views', 'likes', 'CTR']

        for metric in metrics:
            is_alert, current_value, diff = check_anomaly(
                df, metric, threshold=0.3)
            if is_alert:
                send_alert(metric, current_value, diff, df)
            else:
                print(f"✅ {metric}: {current_value:.4f} — норма")

    @task()
    def check_messages():
        q = """
        SELECT
            toStartOfFifteenMinutes(time) as ts,
            toDate(ts) as date,
            formatDateTime(ts, '%R') as hm,
            uniqExact(user_id) as users_messages,
            count() as messages_sent
        FROM simulator_20260520.message_actions
        WHERE ts >= today() - 1
            AND ts < toStartOfFifteenMinutes(now())
        GROUP BY ts, date, hm
        ORDER BY ts
        """
        df = ph.read_clickhouse(q, connection=connection)

        metrics = ['users_messages', 'messages_sent']

        for metric in metrics:
            is_alert, current_value, diff = check_anomaly(
                df, metric, threshold=0.3)
            if is_alert:
                send_alert(metric, current_value, diff, df)
            else:
                print(f"✅ {metric}: {current_value:.0f} — норма")

    check_feed()
    check_messages()

dag_alerts_ilgam = dag_alerts_ilgam()
