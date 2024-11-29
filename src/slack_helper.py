import io
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError

def slack_send_files(file_content, slack_token: str, channel_id: str) -> None:
    """
    Sends a notification to a Slack channel.
    """
    print("sending slack notification")

    client = WebClient(token=slack_token)
    try:
        response = client.files_upload(
            channels=channel_id,
            file=io.BytesIO(file_content),
            filename='reconciliation_report.xlsx',
            initial_comment=f""
        )
        print(f"slack message status code {response.status_code}")

    except SlackApiError as e:
        print(f"Error occurred while uploading file to Slack: {str(e)}")
        raise {'error': str(e)}

def slack_send_table(secret,token):
    from tabulate import tabulate
    import emoji
    import pandas as pd

    channel = []

    df=pd.DataFrame.from_dict(dict).T.sort_values(by=['runs'],ascending=False)
    
    msg_success=tabulate(df,headers=['Job name','# of runs','Avg time','Succeeded','Failed','In progress','Stoplights'],tablefmt="psql",stralign="left")
    msg_success=f"Below is the report for last 24h \n{emoji.emojize(':multiply:')}:no job runs \n{emoji.emojize(':police_car_light:')}:job deleted\n"+"```" + msg_success + "```"
    
    client = WebClient(token=secret[token])

    for channel_name in channel:
        client.chat_postMessage(channel='#'+channel_name,text=msg_success)
        print("msg sent")