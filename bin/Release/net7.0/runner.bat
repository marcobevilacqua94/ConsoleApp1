FOR /L %%i IN (1,1,100) DO (

    echo %%i
    ConsoleApp1.exe localhost Administrator password 5000 10000
)