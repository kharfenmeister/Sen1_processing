#######################################
### Sentinel-1 Processing Running ###
year = "2018"
site = "Demmin"

snappydir = '/home/kheupel/.snap/snap-python'
data_dir = "/misc/klima4/sen1_katharina/" + site + "/" + year
processing_dir = "/misc/klima1/Katharina/AgriFusion/Daten/Sen1/" + site + "/" + year

# Configure Python and SNAP
import sys
sys.path.append(snappydir)
import snappy

sys.path.append("/misc/klima1/Katharina/AgriFusion/Analyse/Sen1/Sen1_processing_snappy/")
import sen1_processing_functions as spf

import os
import stat
import zipfile
import shutil
from snappy import GPF
from snappy import ProductIO
from snappy import HashMap
from snappy import jpy

# Load HashMap to have access to all JAVA operators
snappy.GPF.getDefaultInstance().getOperatorSpiRegistry().loadOperatorSpis()
HashMap = snappy.jpy.get_type('java.util.HashMap')

data_list = [os.path.join(data_dir,f) for f in os.listdir(data_dir) if f.endswith(".zip")]
data_list.sort()

for i in range(78, len(data_list)):
#for i in range(0, 1):
	# Load Image
	sen1_zip = data_list[i]
	sen1_date = os.path.basename(data_list[i]).split("_")[5][:8]
	print(str(i) + "_" + sen1_date)
	
	sen1_img = processing_dir + "/00_Download/" + os.path.splitext(os.path.basename(sen1_zip))[0]+ ".SAFE"
	
	# Unzip
	if not os.path.exists(sen1_img):
		zip = zipfile.ZipFile(sen1_zip, "r")
		zip.extractall(processing_dir + "/00_Download")		
	else:
		print("Already unzipped")
	
	
	# sen1_img = [os.path.join(processing_dir + "/00_Download",f) for f in os.listdir(processing_dir + "/00_Download") if f.endswith(".SAFE")]
	
	# Read Image
	# sen1_img_i = [s for s in sen1_img if sen1_date in s]
	sen1 = ProductIO.readProduct(sen1_img) 
	
	# 01 TOPS Split
	print("01 TOPS Split")
	parameters = HashMap()
	orbit = sen1.getMetadataRoot().getElement('Abstracted_Metadata').getAttribute('REL_ORBIT').getData()
	orbit = int(str(orbit))
	
	if orbit > 100:
		if site == "Demmin":
			iw = "IW2"
		else:
			iw = "IW1"
	else:
		iw = "IW3"
	
	print(orbit)
	print(iw)
	
	parameters.put("subswath", iw)
	parameters.put("selectedPolarisations", "VV,VH")
	tops_split = GPF.createProduct("TOPSAR-Split", parameters, sen1)
	outfile = processing_dir + "/01_TOPS-Split/" + "Sen1_" + sen1_date + "_split"
	# ProductIO.writeProduct(tops_split, outfile, 'BEAM-DIMAP')
	
	# 02 Apply Orbit File
	print("02 Apply Orbit File")
	parameters = HashMap()
	parameters.put("Orbit State Vectors", "Sentinel Precise (Auto Download)")
	parameters.put("Polynomial Degree", 3) 
	applyorbit = GPF.createProduct("Apply-Orbit-File", parameters, tops_split)
	outfile = processing_dir + "/02_ApplyOrbit/" + "Sen1_" + sen1_date + "_orbit"
	#ProductIO.writeProduct(applyorbit, outfile, 'BEAM-DIMAP')
	
	# 03 Calibration
	print("03 Calibration")
	parameters = HashMap()
	parameters.put('outputImageInComplex', True)
	parameters.put('selectedPolarisations', "VV,VH") 
	calibration = GPF.createProduct("Calibration", parameters, applyorbit) 
	outfile = processing_dir + "/03_Calibration/" + "Sen1_" + sen1_date + "_calibration"
	# ProductIO.writeProduct(calibration, outfile, 'BEAM-DIMAP')
	
	# 04 Deburst
	print("04 Deburst")
	parameters = HashMap()
	parameters.put("Polarisations", "VV,VH")
	deburst = GPF.createProduct("TOPSAR-Deburst", parameters, calibration)
	outfile = processing_dir + "/04_Deburst/" + "Sen1_" + sen1_date + "_deburst"
	# ProductIO.writeProduct(deburst, outfile, 'BEAM-DIMAP')
	
	# 05 Polarimetric Speckle Filtering
	print("05 Polarimetric Speckle Filtering")
	parameters = HashMap()
	parameters.put("filter", "Refined Lee Filter")
	parameters.put("windowSize", "5x5")
	polspeck = GPF.createProduct("Polarimetric-Speckle-Filter", parameters, deburst)
	outfile = processing_dir + "/05_PolarimetricSpeckleFilter/" + "Sen1_" + sen1_date + "_polspeck"
	# ProductIO.writeProduct(polspeck, outfile, 'BEAM-DIMAP')
	
	# A Polarimetric Decomposition
	print("A Polarimetric Decomposition")
	parameters = HashMap()
	parameters.put("decomposition", "H-Alpha Dual Pol Decomposition")
	parameters.put("windowSize", 3)
	# parameters.put("outputHAAlpha", True)
	poldec = GPF.createProduct("Polarimetric-Decomposition", parameters, polspeck)
	outfile = processing_dir + "/A_PolarimetricDecomposition/" + "Sen1_" + sen1_date + "_poldec"
	# ProductIO.writeProduct(poldec, outfile, 'BEAM-DIMAP')
	
	# B Subset
	print("B Subset")
	
	if site == "Demmin":
		#wkt = "POLYGON((12.768410408781 53.7185653945843, 12.768410408781 54.1037111914009, 13.4923760399692 54.1037111914009, 13.4923760399692 53.7185653945843, 12.768410408781 53.7185653945843))"
		wkt = "POLYGON((13.1242967749401 53.8104646405203, 13.1242967749401 53.9410622575483, 13.3274653008887 53.9410622575483, 13.3274653008887 53.8104646405203, 13.1242967749401 53.8104646405203))"
	else:
		wkt = "POLYGON((12.8042687122848 51.9169949178169, 12.9994312618759 51.9169949178169, 12.9994312618759 52.0255629427958, 12.8042687122848 52.0255629427958, 12.8042687122848 51.9169949178169))"
		# wkt = "POLYGON((12.8096370950014 51.9526302165024, 12.9994645953145 51.9526302165024, 12.9994645953145 52.0239725178063, 12.8096370950014 52.0239725178063, 12.8096370950014 51.9526302165024))"
	
	
	WKTReader = snappy.jpy.get_type('com.vividsolutions.jts.io.WKTReader')
	shp = WKTReader().read(wkt)
	parameters = HashMap()
	parameters.put('geoRegion', shp)
	parameters.put('outputImageScaleInDb', False)
	subset = GPF.createProduct("Subset", parameters, poldec)
	outfile = processing_dir + "/B_Subset_Spk/" + "Sen1_" + sen1_date + "_subset_pol_spk"
	ProductIO.writeProduct(subset, outfile, 'BEAM-DIMAP')
	
	# # Make unzipped folder "measurement" writable to delete it
	# m_files = [os.path.join(sen1_img + "/measurement/",f) for f in os.listdir(sen1_img + "/measurement/")]
	
	# for x in range(0, len(m_files)):
		# os.chmod(m_files[x], stat.S_IWRITE)
		# os.unlink(m_files[x])
	
	
	# # Remove unzipped folder
	# shutil.rmtree(sen1_img, onerror = on_rm_error)