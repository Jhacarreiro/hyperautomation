# hyperautomation
Script to automate the run of various hyperopt sessions in a row.

Script reads data from the result page from the terminal, the result file created by hyper and the config ghseet. In the future the manual fields from the config gsheet can come from the strategy file and freqtrade config file.

Reads configs from a json file and then check a specific gsheet for the rest of the hyper settings
Writes results to specific gsheet

Configuration of the fields to show in result sheet come from the config file, this are default, the others depend on the strategy:
 "Date and Time", "Run #", "Strategy", "Config", "Epochs", "random-state", "Timerange", "Pairs", "loss_function", "Leverage", "% per trade", 
Results: "Trades #", "% Win", "Avg. Profit %", "Profit %", "Duration min", "DrawDown %"

Make sure to match the config and result headers with the dictionary in the script.

Example config gsheet:
![image](https://github.com/user-attachments/assets/7ce0404c-b97d-4611-8ba7-a2e5c59917d6)

Example result gsheet:
![image](https://github.com/user-attachments/assets/9e5eb35c-63db-41ab-9285-200861692057)
