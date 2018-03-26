sudo mkdir -p /usr/share/bdbm_drv
sudo touch /usr/share/bdbm_drv/ftl.dat
sudo touch /usr/share/bdbm_drv/dm.dat


echo "insmod $1 $2 CP$3"

sudo insmod ../../devices/ramdrive_timing/risa_dev_ramdrive_timing.ko
sudo insmod CP${3}_84/bdbm_drv_CP${3}_${1}_${2}.ko
