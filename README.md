How to run the program

1. cd to Handin-4(root) folder and do the following:
- Terminal 1: go run . -id=1 -addr='127.0.0.1:5001' -peers='1=127.0.0.1:5001,2=127.0.0.1:5002,3=127.0.0.1:5003' -auto
- Terminal 2: go run . -id=2 -addr='127.0.0.1:5002' -peers='1=127.0.0.1:5001,2=127.0.0.1:5002,3=127.0.0.1:5003' -auto
- Terminal 3: go run . -id=3 -addr='127.0.0.1:5003' -peers='1=127.0.0.1:5001,2=127.0.0.1:5002,3=127.0.0.1:5003' -auto

3. The program has to be terminated manually with Ctrl + C in each terminal.
