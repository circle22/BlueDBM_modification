sudo mkdir -p /usr/share/bdbm_drv
sudo touch /usr/share/bdbm_drv/ftl.dat
sudo touch /usr/share/bdbm_drv/dm.dat


echo "insmod $1 CP$2 page $3"

#sudo insmod ../../devices/ramdrive_timing/risa_dev_ramdrive_timing.ko
sudo insmod ../../devices/ramdrive/risa_dev_ramdrive_$3.ko
sudo insmod bdbm_drv_CP${2}_${1}.ko
