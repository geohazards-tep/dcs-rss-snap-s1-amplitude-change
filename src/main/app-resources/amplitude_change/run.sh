#!/bin/bash

# source the ciop functions (e.g. ciop-log, ciop-getparam)
source ${ciop_job_include}

# set the environment variables to use ESA SNAP toolbox
source $_CIOP_APPLICATION_PATH/gpt/snap_include.sh

# define the exit codes
SUCCESS=0
ERR_NODATA=1
SNAP_REQUEST_ERROR=2
ERR_SNAP=3
ERR_WRONGINPUTNUM=4
ERR_WRONG_INPUT_TYPE=5
ERR_WRONG_POLARISATION=6
ERR_WRONG_PIXEL_SPACING=7
ERR_NO_ENCLOSURE=8
ERR_GDAL_MERGE=9
ERR_GDAL=10
ERR_TAR=11
ERR_MST_SLV_DIFFER=12

# add a trap to exit gracefully
function cleanExit ()
{
    local retval=$?
    local msg=""

    case ${retval} in
        ${SUCCESS})               	msg="Processing successfully concluded";;
        ${ERR_NODATA})            	msg="Could not retrieve the input data";;
        ${SNAP_REQUEST_ERROR})    	msg="Could not create snap request file";;
        ${ERR_SNAP})              	msg="SNAP failed to process";;
	${ERR_WRONGINPUTNUM})           msg="The number of input cannot exceed 1";;
	${ERR_WRONG_INPUT_TYPE})        msg="The provided input is not a S1 IW GRD product";;
	${ERR_WRONG_POLARISATION})      msg="Mismatching between input polarisation and parameter";;
	${ERR_WRONG_PIXEL_SPACING})     msg="The provided pixel spacing is not a number";;
	${ERR_NO_ENCLOSURE})            msg="Failed to retrieve enclosure";;
	${ERR_GDAL_MERGE})              msg="Failed to run gdal_merge.py";;
	${ERR_GDAL})                    msg="Failed to run GDAL";;
	${ERR_TAR})                     msg="Failed to run tar";;
	${ERR_MST_SLV_DIFFER})		msg="Master and Slave input types differ";;
        *)                        	msg="Unknown error";;
    esac

   [ ${retval} -ne 0 ] && ciop-log "ERROR" "Error ${retval} - ${msg}, processing aborted" || ciop-log "INFO" "${msg}"
#   if [ $DEBUG -ne 1 ] ; then
#	[ ${retval} -ne 0 ] && hadoop dfs -rmr $(dirname "${inputfiles[0]}")
#   fi
   exit ${retval}
}

trap cleanExit EXIT

###########################################################################################
###### Get inputs and parameters  ######
###########################################################################################

inputfiles=
#get input product list and convert it into an array
while read inputproduct; do
	ciop-log "DEBUG" "Input: $inputproduct"
	if [[ -z "$inputfiles" ]]; then
		inputfiles=$inputproduct
	else
		inputfiles=("${inputfiles[@]}" $inputproduct)
	fi
done

#get the number of products to be processed
inputfilesNum="${#inputfiles[@]}" 
#ciop-log "DEBUG" "Number of input files: $inputfilesNum" 
[ "$inputfilesNum" -ne "1" ] && exit $ERR_WRONGINPUTNUM
      
#defines the inputs (i.e. master and slave products, where the master assumed to be provided as first)
master="${inputfiles[0]}"        

slave="`ciop-getparam slave`"

# Retrieve the parameters value from workflow or job default value
SubsetBoundingBox="`ciop-getparam SubsetBoundingBox`"
ciop-log "DEBUG" "The selected subset bounding box data is: ${SubsetBoundingBox}"

# Retrieve pixel spacing
pixelSpacingInMeter="`ciop-getparam pixelSpacingInMeter`"
ciop-log "DEBUG" "The selected Pixel Spacing is: ${pixelSpacingInMeter}"

# Retrieve polarisation
polarisation="`ciop-getparam polarisation`"
ciop-log "DEBUG" "The selected polarisation is: ${polarisation}"

########################################################################################### 


###########################################################################################
###### Validate inputs and parameters  ######
###########################################################################################
# retrieve master enclosure
master_enclosure="$(opensearch-client $master enclosure)"
res=$?
[ $res -eq 0 ] && [ -z "${master_enclosure}" ] && exit $ERR_NO_ENCLOSURE
[ $res -ne 0 ] && exit $ERR_NO_ENCLOSURE

master_filename="${master_enclosure%/}"
master_filename="${master_filename##*/}"
chk_mst_GRD="${master_filename:7:3}"
[[ "$chk_mst_GRD" == "GRD" ]] || exit $ERR_WRONG_INPUT_TYPE

slave_enclosure="$(opensearch-client $slave enclosure)" 
res=$?
[ $res -eq 0 ] && [ -z "${slave_enclosure}" ] && exit $ERR_NO_ENCLOSURE
[ $res -ne 0 ] && exit $ERR_NO_ENCLOSURE

slave_filename="${slave_enclosure%/}"
slave_filename="${slave_filename##*/}" 
chk_slv_GRD="${slave_filename:7:3}"
[[ "$chk_slv_GRD" == "GRD" ]] || exit $ERR_WRONG_INPUT_TYPE

#Retrieve acquisition mode (IW or EW)
chk_mst_mode="${master_filename:4:2}"
chk_slv_mode="${slave_filename:4:2}"
[[ "$chk_mst_mode" == "$chk_slv_mode" ]] || exit $ERR_MST_SLV_DIFFER
prod_mst_slv_mode="$chk_mst_mode"
if [[ "$prod_mst_slv_mode" != "IW" ]] && [[ "$prod_mst_slv_mode" != "EW" ]]; then 
	exit $ERR_WRONG_INPUT_TYPE
fi

#Retrieve resolution
chk_mst_res="${master_filename:7:4}"
chk_slv_res="${slave_filename:7:4}"
[[ "$chk_mst_res" == "$chk_slv_res" ]] || exit $ERR_MST_SLV_DIFFER
prod_mst_slv_res="$chk_mst_res"
ciop-log "DEBUG" "Checking input product pixel spacing"
input_pixel_spacing=
case ${prod_mst_slv_res} in
	GRDH)	#Check also acquisition mode
		ciop-log "DEBUG" "Retrieving input pixel spacing for GRDH product"
		if [[ "$prod_mst_slv_mode" == "IW" ]]; then
			input_pixel_spacing="10"
		elif [[ "$prod_mst_slv_mode" == "EW" ]]; then
			input_pixel_spacing="25"
		else
			exit $ERR_WRONG_INPUT_TYPE
		fi
		;;
	 GRDM)	#For GRDM IW and EW have the same pixel spacing 
		ciop-log "DEBUG" "Retrieving input pixel spacing for GRDM product"
		input_pixel_spacing="40"
		;;
	*)	ciop-log "DEBUG" "Input product resolution is not GRDH or GRDM"
		exit $ERR_WRONG_INPUT_TYPE
		;;
esac
ciop-log "DEBUG" "Input product pixel spacing is: $input_pixel_spacing "

# Check bounding box
# Perform subsetting if SubsetBoundingBox variable is not empty
perform_subset="true"
if [[ -z "$SubsetBoundingBox" ]]; then
	perform_subset="false"
else
	# Put BoundingBox into array
	coords=($(echo "$SubsetBoundingBox" | tr ',' ' '))
	lon_min=${coords[0]}
	lat_min=${coords[1]}
	lon_max=${coords[2]}
	lat_max=${coords[3]}
	# Define subsetting WKT polygon
	subset_wkt_polygon="POLYGON(( $lon_min $lat_min, $lon_max $lat_min, $lon_max $lat_max, $lon_min $lat_max, $lon_min $lat_min ))"
fi

# Check polarisation
chk_mst_pol="${master_filename:14:2}"
chk_slv_pol="${slave_filename:14:2}"
case ${polarisation} in
	VV)	# check Master Polarisation
		ciop-log "DEBUG" "Checking Master Polarisation"
		if [[ "$chk_mst_pol" != "SV" ]] && [[ "$chk_mst_pol" != "DV" ]]; then
			exit ${ERR_WRONG_POLARISATION}
		fi
		# check Slave Polarisation
		ciop-log "DEBUG" "Checking Slave Polarisation"
                if [[ "$chk_slv_pol" != "SV" ]] && [[ "$chk_slv_pol" != "DV" ]]; then
                        exit ${ERR_WRONG_POLARISATION}
                fi
		;;
	VH)     # check Master Polarisation
		ciop-log "DEBUG" "Checking Master Polarisation"
		if [[ "$chk_mst_pol" != "DV" ]]; then
			exit ${ERR_WRONG_POLARISATION}
		fi
                # check Slave Polarisation
		ciop-log "DEBUG" "Checking Slave Polarisation"
		if [[ "$chk_slv_pol" != "DV" ]]; then
                        exit ${ERR_WRONG_POLARISATION}
                fi
		;;
	HH)	# check Master Polarisation
		ciop-log "DEBUG" "Checking Master Polarisation"
		if [[ "$chk_mst_pol" != "SH" ]] && [[ "$chk_mst_pol" != "DH" ]]; then
                        exit ${ERR_WRONG_POLARISATION}
                fi
                # check Slave Polarisation
                ciop-log "DEBUG" "Checking Slave Polarisation"
                if [[ "$chk_slv_pol" != "SH" ]] && [[ "$chk_slv_pol" != "DH" ]]; then
                        exit ${ERR_WRONG_POLARISATION}
                fi
                ;;
	HV)	# check Master Polarisation
                ciop-log "DEBUG" "Checking Master Polarisation"
                if [[ "$chk_mst_pol" != "DH" ]]; then
                        exit ${ERR_WRONG_POLARISATION}
                fi
                # check Slave Polarisation
                ciop-log "DEBUG" "Checking Slave Polarisation"
                if [[ "$chk_slv_pol" != "DH" ]]; then
                        exit ${ERR_WRONG_POLARISATION}
                fi
                ;;
	*)	ciop-log "DEBUG" "Unknown Polarisation"
		exit ${ERR_WRONG_POLARISATION}
		;;
esac

# Check pixel spacing
ciop-log "INFO" "Checking Pixel Spacing"
if [[ ! "$pixelSpacingInMeter" =~ ^[0-9]*\.?[0-9]*$ ]]; then
	exit ${ERR_WRONG_PIXEL_SPACING}
fi

#Check if multilooking is needed and define multilooking factor
ciop-log "INFO" "Checking if multilooking is needed"
perform_multilook="false"
multilook_factor=
pixelSpacingInMeter_int="${pixelSpacingInMeter%%.*}"
if [[ "$pixelSpacingInMeter_int" -gt "$input_pixel_spacing" ]]; then
	multilook_factor="$(( $pixelSpacingInMeter_int / $input_pixel_spacing ))"
	ciop-log "DEBUG" "PixelSpacing(int): $pixelSpacingInMeter_int ; InputProductPixelSpacing: $input_pixel_spacing ; ML factor: $multilook_factor " 
	if [[ "$multilook_factor" -gt "1" ]]; then 
		perform_multilook="true"
	fi
fi
if [[ "$perform_multilook" == "true" ]]; then
	ciop-log "INFO" "Multilooking is needed. ML factor = $multilook_factor "
else
	ciop-log "INFO" "Multilooking is not needed."
fi

###########################################################################################


###########################################################################################
###### Retrieve input data  ######
###########################################################################################
# Retrieve master product
ciop-log "INFO" "Retrieving master product"
ciop-log "INFO" "Master product enclosure:  ${master_enclosure} "
mkdir ${TMPDIR}/master
master_local_file="$( echo ${master_enclosure} | ciop-copy -f -U -O ${TMPDIR}/master/ - 2> ${TMPDIR}/ciop_copy.stderr )"
res=$?
[ ${res} -ne 0 ] && exit ${ERR_NODATA}

if [[ -d "$master_local_file" ]]; then 
	master_local_file=$(find ${master_local_file}/ -name 'manifest.safe')
fi

# Retrieve slave product
ciop-log "INFO" "Retrieving slave product"
ciop-log "INFO" "Slave product enclosure:  ${slave_enclosure} "
mkdir ${TMPDIR}/slave
slave_local_file="$( echo ${slave_enclosure} | ciop-copy -f -U -O ${TMPDIR}/slave/ - 2> ${TMPDIR}/ciop_copy.stderr )"
res=$?
[ ${res} -ne 0 ] && exit ${ERR_NODATA}

if [[ -d "$slave_local_file" ]]; then
	slave_local_file=$(find ${slave_local_file}/ -name 'manifest.safe')
fi

###########################################################################################


###########################################################################################
###### Build SNAP processing graph  ######
###########################################################################################
# Define gpt template to be used
ciop-log "INFO" "Building SNAP graph request xml"
SNAP_gpt_template=
if [[ "$perform_subset" == "true" ]]; then
	if [[ "$perform_multilook" == "true" ]]; then
		SNAP_gpt_template="$_CIOP_APPLICATION_PATH/amplitude_change/templates/Orb_Cal_ML_TC_Sub_BM_BM_Stack_Graph.xml"
	else
		SNAP_gpt_template="$_CIOP_APPLICATION_PATH/amplitude_change/templates/Orb_Cal_TC_Sub_BM_BM_Stack_Graph.xml"
	fi
else
	if [[ "$perform_multilook" == "true" ]]; then
		SNAP_gpt_template="$_CIOP_APPLICATION_PATH/amplitude_change/templates/Orb_Cal_ML_TC_BM_BM_Stack_Graph.xml"
	else
		SNAP_gpt_template="$_CIOP_APPLICATION_PATH/amplitude_change/templates/Orb_Cal_TC_BM_BM_Stack_Graph.xml"
	fi
fi


# Define output filenames
mkdir -p ${TMPDIR}/output
export OUTPUTDIR=${TMPDIR}/output
master_date="${master_filename:17:8}"
slave_date="${slave_filename:17:8}"
outputname="S1_GRD_${polarisation}_${master_date}_${slave_date}_RGB_Amplitude_Change.tif"
OUTPUTFILE="${OUTPUTDIR}/${outputname}"
rgb_outputname="${outputname%.tif}.RGB.tif"
MASTER_SIGMA="${TMPDIR}/${master_filename%%.*}.tif"
SLAVE_SIGMA="${TMPDIR}/${slave_filename%%.*}.tif"
MASTER_OUTPUT_PHYSICAL="${OUTPUTDIR}/${master_filename%%.*}_Orb_Cal_ML_TC_DB.tif"
SLAVE_OUTPUT_PHYSICAL="${OUTPUTDIR}/${slave_filename%%.*}_Orb_Cal_ML_TC_DB.tif"
MASTER_OUTPUT="${OUTPUTDIR}/${master_filename%%.*}_Orb_Cal_ML_TC_DB.RGB.tif"
SLAVE_OUTPUT="${OUTPUTDIR}/${slave_filename%%.*}_Orb_Cal_ML_TC_DB.RGB.tif"
STACK_RESCALED="${TMPDIR}/BM_stack_rescaled.tif"
RGB_LEGEND="${OUTPUTDIR}/${rgb_outputname%%.*}.legend.png"
BROWSE_RGB_PNG="${OUTPUTDIR}/${rgb_outputname%.tif}.png"

# Legend  template
LEGEND_TEMPLATE="$_CIOP_APPLICATION_PATH/amplitude_change/snac.legend.png"


# Fill SNAP gpt template request
SNAP_gpt_request="${TMPDIR}/Graph.xml"
sed -e "s|%%MASTER%%|${master_local_file}|g" \
    -e "s|%%SLAVE%%|${slave_local_file}|g" \
    -e "s|%%POLYGON_SUBSET%%|${subset_wkt_polygon}|g" \
    -e "s|%%polarisation%%|${polarisation}|g" \
    -e "s|%%pixelSpacingInMeter%%|${pixelSpacingInMeter}|g" \
    -e "s|%%multilook_factor%%|${multilook_factor}|g" \
    -e "s|%%MASTER_SIGMA%%|${MASTER_SIGMA}|g" \
    -e "s|%%SLAVE_SIGMA%%|${SLAVE_SIGMA}|g" \
    -e "s|%%MASTER_OUTPUT_PHYSICAL%%|${MASTER_OUTPUT_PHYSICAL}|g" \
    -e "s|%%SLAVE_OUTPUT_PHYSICAL%%|${SLAVE_OUTPUT_PHYSICAL}|g" \
    -e "s|%%STACK_RESCALED%%|${STACK_RESCALED}|g" \
    -e "s|%%OUTPUT%%|${OUTPUTFILE}|g" $SNAP_gpt_template > $SNAP_gpt_request

###########################################################################################


###########################################################################################
###### Execute SNAP processing graph  ######
###########################################################################################
#Run SNAP workflow
ciop-log "INFO" "Execute SNAP processing graph"
gpt $SNAP_gpt_request -c "${CACHE_SIZE}" #&> /dev/null
res=$?
[ $res -ne 0 ] && exit ${ERR_SNAP}

# Run gdal_merge.py to generate RGB composite
RGB_TEMP="${TMPDIR}/rgb_temp_amp_change.tif"

ciop-log "INFO" "Run gdal_merge.py to stack the tif files"
STACK_RESCALED_2="${STACK_RESCALED%.*}_2.tif"
gdal_merge.py -separate -n 0 -co "ALPHA=YES" ${MASTER_SIGMA} ${SLAVE_SIGMA} -o ${STACK_RESCALED_2}
[ $? -eq 0 ] || exit ${ERR_GDAL}

SNAP_gpt_common="$_CIOP_APPLICATION_PATH/amplitude_change/templates/common_area_graph.xml"
SNAP_gpt_common_request="${TMPDIR}/common_area_graph.xml"
OUTPUTSTACK="${TMPDIR}/OUTPUTSTACK.tif"
sed -e "s|%%STACK_RESCALED_2%%|${STACK_RESCALED_2}|g" \
    -e "s|%%OUTPUTSTACK%%|${OUTPUTSTACK}|g" $SNAP_gpt_common > $SNAP_gpt_common_request

ciop-log "INFO" "Execute SNAP processing graph common area"
gpt $SNAP_gpt_common_request -c "${CACHE_SIZE}" #&> /dev/null
res=$?
[ $res -ne 0 ] && exit ${ERR_SNAP}
 
ciop-log "INFO" "Run gdal_translate to generate RGB Tiff"
gdal_translate -ot Byte -of GTiff -b 2 -b 1 -b 1 -a_nodata 0 -co "PHOTOMETRIC=RGB" -co "ALPHA=YES" ${OUTPUTSTACK} ${RGB_TEMP}
[ $? -eq 0 ] || exit ${ERR_GDAL}


# gdal_warp
ciop-log "INFO" "Running gdalwarp for RGB product"
gdalwarp -ot Byte -t_srs EPSG:3857 -srcnodata 0 -dstnodata 0 -dstalpha -co "ALPHA=YES" ${RGB_TEMP} ${OUTPUTDIR}/${rgb_outputname}
returnCode=$?
[ $returnCode -eq 0 ] || exit ${ERR_GDAL}

# Create PNG
ciop-log "INFO" "Run gdal_translate to generate Browse PNG"
gdal_translate -of PNG ${OUTPUTDIR}/${rgb_outputname} $BROWSE_RGB_PNG
returnCode=$?
[ $returnCode -eq 0 ] || exit ${ERR_GDAL}

#Add overviews gdaladdo
ciop-log "INFO" "Running gdaladdo for RGB product"
gdaladdo -r average ${OUTPUTDIR}/${rgb_outputname} 2 4 8 16
returnCode=$?
[ $returnCode -eq 0 ] || exit ${ERR_GDAL}

#Copy legend template into the RGM output legend
cp -f $LEGEND_TEMPLATE $RGB_LEGEND

#Remove PNG aux.xml file
if [[ -e "${BROWSE_RGB_PNG}.aux.xml" ]]; then
	rm -f ${BROWSE_RGB_PNG}.aux.xml
fi

# Run gdal commands Master product
# gdal_warp
ciop-log "INFO" "Running gdalwarp for Master product"
gdalwarp -ot Byte -t_srs EPSG:3857 -srcnodata 0 -dstnodata 0 -dstalpha -co "ALPHA=YES" ${MASTER_SIGMA} ${MASTER_OUTPUT}
returnCode=$?
[ $returnCode -eq 0 ] || exit ${ERR_GDAL}
#Add overviews gdaladdo
ciop-log "INFO" "Running gdaladdo for Master product"
gdaladdo -r average ${MASTER_OUTPUT} 2 4 8 16
returnCode=$?
[ $returnCode -eq 0 ] || exit ${ERR_GDAL}
rm -f ${MASTER_SIGMA}

# Run gdal commands Slave product
# gdal_warp
ciop-log "INFO" "Running gdalwarp for Slave product"
gdalwarp -ot Byte -t_srs EPSG:3857 -srcnodata 0 -dstnodata 0 -dstalpha -co "ALPHA=YES" ${SLAVE_SIGMA} ${SLAVE_OUTPUT}
returnCode=$?
[ $returnCode -eq 0 ] || exit ${ERR_GDAL}
#Add overviews gdaladdo
ciop-log "INFO" "Running gdaladdo for Slave product"
gdaladdo -r average ${SLAVE_OUTPUT} 2 4 8 16
returnCode=$?
[ $returnCode -eq 0 ] || exit ${ERR_GDAL}
rm -f ${SLAVE_SIGMA}


# Remove Stack Tiff output not desired
ciop-log "INFO" "Removing Stack Tiff product ${OUTPUTFILE##*/}"
rm -f $OUTPUTFILE

###########################################################################################


###########################################################################################
###### Create .properties files  ######
###########################################################################################
processing_time=$(date)
# RGB product properties file 
cat <<EOF > ${OUTPUTDIR}/${rgb_outputname%%.*}.properties
title=${outputname}
Description=Amplitude Change RGB Composite - R=Slave G=Master B=Master 
Master_Product=${master_filename%%.*}
Slave_Product=${slave_filename%%.*}
Master_Date=${master_date}
Slave_Date=${slave_date}
Polarisation=${polarisation}
Pixel_Spacing=${pixelSpacingInMeter}
Processing_Time=${processing_time}
EOF

# Master product properties file
cat <<EOF > ${MASTER_OUTPUT%.RGB.tif}.properties
title=${MASTER_OUTPUT_PHYSICAL##*/}
Description=Master Product 
Master_Product=${master_filename%%.*}
Master_Date=${master_date}
Polarisation=${polarisation}
Pixel_Spacing=${pixelSpacingInMeter}
Processing_Time=${processing_time}
EOF

# Slave product properties file
cat <<EOF > ${SLAVE_OUTPUT%.RGB.tif}.properties
title=${SLAVE_OUTPUT_PHYSICAL##*/}
Description=Slave Product
Slave_Product=${slave_filename%%.*}
Slave_Date=${slave_date}
Polarisation=${polarisation}
Pixel_Spacing=${pixelSpacingInMeter}
Processing_Time=${processing_time}
EOF

###########################################################################################


###########################################################################################
###### Publish SNAP processing results  ######
###########################################################################################
ciop-log "INFO" "Publishing Output Products" 
ciopPublishOut=$( ciop-publish -m "${OUTPUTDIR}"/* )
# cleanup temp dir and output dir 
rm -rf  "${TMPDIR}"/* "${OUTPUTDIR}"/* 

###########################################################################################

exit $SUCCESS

