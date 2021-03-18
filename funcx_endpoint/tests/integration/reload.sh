pushd .
cd ~/src/funcx
./reinstall.sh
popd
echo "Removing $PWD/HighThroughputExecutor"
rm -rf $PWD/HighThroughputExecutor
killall -u $USER funcx-worker -9
killall -u $USER funcx-manager -9
