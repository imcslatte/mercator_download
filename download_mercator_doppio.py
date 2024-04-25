#!/usr/bin/env python
#
import os,sys,fnmatch,glob
from shutil import copyfile
from datetime import datetime,timedelta,date
import subprocess,shutil,os,glob
import copernicusmarine as copernicus_marine
import pandas as pd

outdir='/home/om/cron/mercator/data_doppio_cloud/'
fdir='/home/om/dods-data/thredds/roms/projects/mercator/mercatorPSY4QV3R1/'
tmpdir='/home/om/cron/mercator/data_doppio_cloud/data_doppio_cloud_tmp/'
days=14



latmin=31.0
latmax=48.0
lonmin=-82.0
lonmax=-59.0
zmin=-10.0;
zmax=6000.0
variables=[['so'] ,['thetao'],['uo','vo'],['zos']]
tmpfiles=['TMP_so.nc','TMP_thetao.nc','TMP_uv.nc','TMP_zos.nc']

ids=['cmems_mod_glo_phy-so_anfc_0.083deg_P1D-m', 
     'cmems_mod_glo_phy-thetao_anfc_0.083deg_P1D-m', 
     'cmems_mod_glo_phy-cur_anfc_0.083deg_P1D-m', 
     'cmems_mod_glo_phy_anfc_0.083deg_P1D-m']

# motuclient -u $user -p $paswd -m $motu -s $service_id -d cmems_mod_glo_phy-so_anfc_0.083deg_P1D-m -x $lonmin -X $lonmax  -y $latmin -Y $latmax  -t $timemin -T $timemax -z $zmin -Z $zmax -v so -o $tdir -f mercator_TMP_so.nc
#     cp "$tdir"mercator_TMP_so.nc $ofile
# 	motuclient -u $user -p $paswd -m $motu -s $service_id -d cmems_mod_glo_phy-thetao_anfc_0.083deg_P1D-m -x $lonmin -X $lonmax  -y $latmin -Y $latmax  -t $timemin -T $timemax -z $zmin -Z $zmax -v thetao -o $tdir -f mercator_TMP_thetao.nc
# 	ncks -A "$tdir"mercator_TMP_thetao.nc  $ofile
#     motuclient -u $user -p $paswd -m $motu -s $service_id -d cmems_mod_glo_phy-cur_anfc_0.083deg_P1D-m -x $lonmin -X $lonmax  -y $latmin -Y $latmax  -t $timemin -T $timemax -z $zmin -Z $zmax -v uo -v vo -o $tdir -f mercator_TMP_uv.nc
#     ncks -A "$tdir"mercator_TMP_uv.nc $ofile
# 	motuclient -u $user -p $paswd -m $motu -s $service_id -d cmems_mod_glo_phy_anfc_0.083deg_P1D-m -x $lonmin -X $lonmax  -y $latmin -Y $latmax  -t $timemin -T $timemax -z $zmin -Z $zmax -v zos -o $tdir -f mercator_TMP_zos.nc
# 	ncks -A "$tdir"mercator_TMP_zos.nc $ofile 
# 	
# 	rm /home/om/cron/mercator/tmp/mercator_TMP*.nc
    
    
tstart=date.today()-timedelta(days=14)
tend=date.today()+timedelta(days=7)

#tstart=date.today()-timedelta(days=0)
#tend=date.today()+timedelta(days=1)
days=pd.date_range(tstart,tend)

#mss : mss dtu15

def main(argv):
    print('MAIN')
    for day in days:
        print(day)

        for f in glob.glob(tmpdir+'TMP*.nc'):
            os.remove(f)
            
        for ind,variable in enumerate(variables):
            print(tmpfiles[ind])
            print(ids[ind])
            day2=day+timedelta(days=0.5)
            
            
            copernicus_marine.subset(
                dataset_id = ids[ind],
                variables = variable,
                minimum_longitude = lonmin,
                maximum_longitude = lonmax,
                minimum_latitude = latmin,
                maximum_latitude = latmax,
                minimum_depth = zmin,
                maximum_depth = zmax,
                start_datetime = day.strftime('%Y-%m-%d %H:%M:%S'),
                end_datetime = day2.strftime('%Y-%m-%d %H:%M:%S'),
                output_filename = tmpfiles[ind],
                force_download=True,
                overwrite_output_data =True,
                output_directory = tmpdir,
                )
        
        ofilename = day.strftime(outdir+'mercator_doppio_%Y_%m_%d.nc')
        ofilename2 = day.strftime(fdir+'mercator_doppio_%Y_%m_%d.nc')
        tmpofilename = day.strftime(tmpdir+'mercator_doppio_%Y_%m_%d.nc')
        print(ofilename)
        
        try:
            print('Copying: '+tmpdir+tmpfiles[0]+' to '+tmpofilename)
            shutil.copyfile(tmpdir+tmpfiles[0], tmpofilename)
    
            
            for tmpfile in tmpfiles[1:]:
                print('Adding: '+tmpdir+tmpfile+' to '+tmpofilename)
                subprocess.check_call(['ncks','-A',tmpdir+tmpfile,tmpofilename])
        except Exception as e:
                # Code to handle other exceptions (if any)
            print(f"An error occurred when combining files via ncks: {e}")
            
            if os.path.exists(tmpofilename):
                print(f'Deleting: {tmpofilename}') 
        else:
            shutil.copyfile(tmpofilename,ofilename)
            shutil.copyfile(tmpofilename,ofilename2)
            

if __name__ == "__main__":

    
    print('RUNNING')
    main(sys.argv)
    for f in glob.glob(tmpdir+'*.nc'):
        os.remove(f)
    for f in glob.glob(tmpdir+'*.tmp'):
        os.remove(f)
