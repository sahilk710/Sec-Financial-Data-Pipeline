FROM apache/airflow:2.8.1

USER root
RUN apt-get update &&     apt-get install -y git python3-pip

USER airflow
RUN pip3 install dbt-snowflake==1.5.0

ENV PATH="/home/airflow/.local/bin:/Users/sahilkasliwal/Library/Python/3.11/bin:/Users/sahilkasliwal/google-cloud-sdk/bin:/Users/sahilkasliwal/google-cloud-sdk/bin:/Library/Frameworks/Python.framework/Versions/3.11/bin:/usr/local/bin:/System/Cryptexes/App/usr/bin:/usr/bin:/bin:/usr/sbin:/sbin:/var/run/com.apple.security.cryptexd/codex.system/bootstrap/usr/local/bin:/var/run/com.apple.security.cryptexd/codex.system/bootstrap/usr/bin:/var/run/com.apple.security.cryptexd/codex.system/bootstrap/usr/appleinternal/bin:/Library/Apple/usr/bin:/usr/local/share/dotnet:~/.dotnet/tools:/Library/Frameworks/Mono.framework/Versions/Current/Commands:/Users/sahilkasliwal/Library/Python/3.11/bin:/Users/sahilkasliwal/google-cloud-sdk/bin:/opt/anaconda3/bin:/opt/anaconda3/condabin:/Library/Frameworks/Python.framework/Versions/3.11/bin:/usr/local/mysql-8.0.28-macos11-x86_64/bin:/usr/local/mysql-8.0.28-macos11-x86_64/bin"
