./run.sh1 clear_onebox
./run.sh2 clear_onebox
./run.sh1 start_onebox
./run.sh2 start_onebox
sleep 10
echo "create tyz" | ./run.sh1 shell
cd /home/smilencer/Code/multi_client1/
go run main.go


