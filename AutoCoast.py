import paramiko
import pandas as pd

ssh = paramiko.SSHClient()
ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())

source_ip = "147.45.254.148"
source_username = "root"
source_password = "u-W??cw#Vr7iMS"
ssh.connect(source_ip, username=source_username, password=source_password)
sftp = ssh.open_sftp()
file_to_transfer = '/root/PlushPit/PlushPitFinance/price_list.csv'
destination_path = 'price_list.csv'

sftp.get(file_to_transfer, destination_path)
sftp.close()
ssh.close()
df = pd.read_csv("price_list.csv").drop(columns='Unnamed: 0')
print(df)