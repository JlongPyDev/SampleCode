"""
Script: reollcect Waste Collection Pickup Day application data import and update process
Purpose: Import recollect collection day map application with consitently updated data after new parcels are assigned
a pickup day by recollect
Author: Jeff Long
"""

# MONDAY DL # RUN script T NOON ON MONDAYS  #  YARDWASTE # dont display sub divs that dont have parcel pts
try:
    import arcpy, os, traceback, sys, glob, pathlib2, datetime, re
    from collections import defaultdict
    from start_stop_mapservice import stopStartServices
    from utils import automated_emails
    from utils import logger
    from utils import sde_functions
except Exception as e:
    print e



def email(x): automated_emails.auto_email(["Jeff.Long@apexnc.org"], subject="ERROR DETECTED: Recollect Data "
                                                                            "Import/Ovewrite/Dataupload"
                                                                            "Script", text=str(x))


now = datetime.datetime.now().strftime('%Y%m%d_%H_Hours_%M_Mins_%S_Sec')
arcpy.env.overwriteOutput = 1
arcpy.Delete_management("in_memory")
out_direct = r"\\Apexgis\GIS\recollect_appdata"
pub_db =r"C:\Users\Jlong\AppData\Roaming\ESRI\Desktop10.5\ArcCatalog\APEXPUBLISHER_jlong.sde"
arcpy.env.workspace = pub_db


log = logger.init_logging(r"\\Apexgis\GIS\recollect_appdata\logfiles", "RecollectDataUpload")
log.info("Initializing Log:%s" % log)
success_msg = "---SUCCESSFUL EXECUTION---"
fail_msg = "---FAILURE---"





try:
    par_csv = None
    if len(glob.glob(r'\\Apexgis\GIS\recollect_appdata\csv_download\*.csv')) > 1:
        print "There should only be the most current CSV file in the directory"
        log.warning("More than 1 CSV detected in csv_download network directory")
    else:
        print "Current Recollect Parcels CSV detected"
        par_csv = glob.glob(r'\\Apexgis\GIS\recollect_appdata\csv_download\*.csv')[0]
        log.info("Verify Current Recollect Parcels as only CSV {}".format(success_msg))
except Exception as e:
    tb = traceback.format_exc()
    email(tb)
    log.warning("Verify Current Recollect Parcels as only CSV {}".format(fail_msg), tb)
    raise sys.exit()


try:
    sde_functions.disconnect_db_users('WCD',arcpy.env.workspace)
except Exception as e:
    tb = traceback.format_exc()
    email(tb)
    raise sys.exit()


# SDE FEATURE CLASSES
pub_sde_db_form = r"C:\Users\Jlong\AppData\Roaming\ESRI\Desktop10.5\ArcCatalog\APEXPUBLISHER.sde\APEXPUBLISHER" \
                  r".DBO.{}"

apxsubdivs = r'C:\Users\Jlong\AppData\Roaming\ESRI\Desktop10.5\ArcCatalog\APEXPL.sde\APEXPL.PL.Subdivisions\APEXPL.PL' \
             r'.ApexDevelopment_Residential'

apx_clipper = r'C:\Users\Jlong\AppData\Roaming\ESRI\Desktop10.5\ArcCatalog\APEXWCD.sde\APEXWCD.WCD.WakeCountyApex' \
              r'\APEXWCD.WCD.apex_clipper'

# IN MEMORY FEATURECLASSES FOR FASTER RUNTIME PERFORMANCE
tempfc_subs = r'in_memory\recollect_subdivisions'


arcpy.FeatureClassToFeatureClass_conversion(apxsubdivs, os.path.dirname(tempfc_subs), os.path.basename(
    tempfc_subs))

log.info("Convert sde subdivisions to in_mem feature class {}".format(success_msg))

# GLOBAL DICTIONARY STORES
days_dict = {'Mon': 'Monday', 'Wed':'Wednesday','Fri': 'Friday','Tue': 'Tuesday', 'Thu': 'Thursday'}
data_dict = {}
assign_dict = {}

# APEXGIS SERVER PARAMETERS
server = r'apexgis'
port='6080'
adminUser='#'
adminPass='#'


def correct_subnames(subdivs):
    try:
        with arcpy.da.UpdateCursor(subdivs, ['Class']) as ucur:
            for row in ucur:
                row[0] = re.sub(r"[']", '', str(row[0]))
                ucur.updateRow(row)

    except Exception as e:
        tb = traceback.format_exc()
        email(tb)
        raise sys.exit()


def assign_pickup_day (subdivs, coll_grid):
    correct_subnames(subdivs)

    arcpy.MakeFeatureLayer_management(subdivs, "sub_lyr")
    arcpy.SelectLayerByLocation_management("sub_lyr", "INTERSECT", coll_grid, selection_type='NEW_SELECTION')
    arcpy.SelectLayerByLocation_management("sub_lyr", "INTERSECT", coll_grid, selection_type='SWITCH_SELECTION')
    arcpy.DeleteFeatures_management("sub_lyr")

    arcpy.SelectLayerByAttribute_management("sub_lyr", "CLEAR_SELECTION")

    arcpy.SelectLayerByLocation_management("sub_lyr", "INTERSECT", coll_grid, selection_type='NEW_SELECTION')

    sub_names_list = [row[0] for row in arcpy.da.SearchCursor("sub_lyr", ['Class'])]



    try:
        arcpy.AddField_management(subdivs, "Trash_and_Recycling_Day", "TEXT")
        arcpy.AddField_management(subdivs, "YardWasteDay", "TEXT")
        arcpy.AddField_management(coll_grid, "Trash_and_Recycling_Day", "TEXT")
        arcpy.AddField_management(subdivs, "Current", "TEXT")
        arcpy.MakeFeatureLayer_management(subdivs, "SubDivs")
        arcpy.MakeFeatureLayer_management(coll_grid, "RecGrid")

    except Exception as e:
        tb = traceback.format_exc()
        email(tb)
        raise sys.exit()

    try:
        with arcpy.da.UpdateCursor(coll_grid, ['Recycling', "Trash_and_Recycling_Day"]) as ucur:
            for row in ucur:
                if row[0] in days_dict.keys():
                    row[1] = days_dict[row[0]]
                    ucur.updateRow(row)
    except Exception as e:
        tb = traceback.format_exc()
        email(tb)
        raise sys.exit()


    #print sub_names_list
    for sub_name in sorted(sub_names_list):
        #print str(sub_name), type(sub_name)
        try:
            day_dict = defaultdict(int)
            arcpy.SelectLayerByAttribute_management("SubDivs", "NEW_SELECTION", "\"Class\" = '{}'".format(sub_name) )
            #print [row[0] for row in arcpy.da.SearchCursor("SubDivs", ['Class'])]

            arcpy.SelectLayerByLocation_management("RecGrid", "INTERSECT", "SubDivs", selection_type='NEW_SELECTION')
            count = int(arcpy.GetCount_management("RecGrid").getOutput(0))
        except Exception as e:
            tb = traceback.format_exc()
            email(tb)
            raise sys.exit()

        print sub_name, "-----", [row[0] for row in arcpy.da.SearchCursor("RecGrid", ['Street'])], count

        print [f.name for f in arcpy.ListFields("RecGrid")]

        try:
            with arcpy.da.SearchCursor("RecGrid", ["Trash_and_Recycling_Day"]) as scur:
                for row in scur:

                    day_dict[row[0]] += 1

            for k, v in day_dict.items():

                print k, v

        except Exception as e:
            tb = traceback.format_exc()
            email(tb)
            raise sys.exit()

        v = list(day_dict.values())
        k = list(day_dict.keys())
        print k
        print v
        try:

            major_day = k[v.index(max(v))]
            #print "MAJOR DAYYYYYYYYYY", major_day, type(major_day)

            assign_dict[str(sub_name)] = str(major_day)

        except Exception as e:
            tb = traceback.format_exc()
            email(tb)
            raise sys.exit()

    log.info("ASSIGNING COLLECTION DAYS")
    for k, v in assign_dict.items():
        #print type(k), k, type(v), v
        log.info("\t Subdivison Collection Day Assigned values {}--{}--{}--{}".format(k, type(k), v, type(v)))

    return assign_dict


def attrib_collect(subdivs, results):

    try:
        with arcpy.da.UpdateCursor(subdivs, ["Class", "Trash_and_Recycling_Day"]) as ucur:
            for row in ucur:
                if row[0] in results.keys():
                    row[1] = str(results[str(row[0])])
                    ucur.updateRow(row)
    except Exception as e:
        tb = traceback.format_exc()
        email(tb)
        raise sys.exit()


def convert_csv_shp(in_csv):

    try:
        sr = 4269
        CoordSys = arcpy.SpatialReference(sr)

        out_Layer = "parcels_waste_lyr"
        arcpy.MakeXYEventLayer_management(in_csv, "Longitude", "Latitude", out_Layer, CoordSys)

        tempfc_par= r'in_memory\pars'
        arcpy.Select_analysis("parcels_waste_lyr", tempfc_par)

        tempfc_rec = r'in_memory\recyclegrid'
    except Exception as e:
        tb = traceback.format_exc()
        email(tb)
        raise sys.exit()

    try:
        arcpy.MakeFeatureLayer_management(tempfc_par, 'parclean_lyr')
        arcpy.SelectLayerByLocation_management('parclean_lyr', 'INTERSECT', select_features=apx_clipper)
        arcpy.Select_analysis('parclean_lyr', tempfc_rec)
        data_dict["rec_mem_fc"] = tempfc_rec
        #yield days_dict["rec_mem_fc"]
    except Exception as e:
        tb = traceback.format_exc()
        email(tb)
        raise sys.exit()


def main():
    try:
        convert_csv_shp(par_csv)
        log.info("Recollect Parcels CSV conversion to GIS format {}".format(success_msg))
        correct_subnames(tempfc_subs)
        log.info("correct subnames function {}".format(success_msg))
    except Exception as e:
        tb = traceback.format_exc()
        email(tb)
        log.warning("Recollect Parcels CSV conversion to GIS format {}".format(fail_msg))
        raise sys.exit()

    try:
        func_results = assign_pickup_day(tempfc_subs, data_dict["rec_mem_fc"])
        log.info("Assign Pickup days function {}".format(success_msg))
        attrib_collect(tempfc_subs, func_results)

    except Exception as e:
        tb = traceback.format_exc()
        email(tb)
        log.warning("{}----{}".format(e,tb))
        raise sys.exit()

    try:
        stopStartServices(server=server, port=port, adminUser=adminUser, adminPass=adminPass,
                          stopStart='Stop', serviceList=['RecollectApplicationData/Recollect.MapServer'])
        log.info("StopServices Function {}".format(success_msg))
    except Exception as e:
        tb = traceback.format_exc()
        email(tb)
        log.warning("StopServices {}".format(fail_msg), tb)
        raise sys.exit()

    try:
        log.info("Attribute collection days function {}".format(success_msg))
        arcpy.CopyFeatures_management(data_dict["rec_mem_fc"], pub_sde_db_form.format('recollect_parcels'))
        log.info("Overwrite recollect parcels to APEXPUBLISHER SDE {}".format(success_msg))
    except Exception as e:
        tb = traceback.format_exc()
        email(tb)
        log.warning("StopServices {}".format(fail_msg), tb)
        raise sys.exit()

    try:
        with arcpy.da.UpdateCursor(tempfc_subs, ["Current"]) as ucur:
            for row in ucur:
                row[0] = str(now)
                ucur.updateRow(row)
        log.info("Time Stamp Now write to Subdivs Row {}".format(success_msg))
        arcpy.CopyFeatures_management(tempfc_subs, pub_sde_db_form.format('recollect_subdivisions'))
        log.info("Overwrite recollect subdivs to APEXPUBLISHER SDE {}".format(success_msg))
        #arcpy.EnableEditorTracking_management(pub_sde_db_form.format('recollect_subdivisions'), "Creator",
        # "Created", "Editor", "Edited", "ADD_FIELDS", "DATABASE_TIME")
        log.info("Requesting to restart RecollectApplicationData/Recollect.MapServer WebService")
        stopStartServices(server=server, port=port, adminUser=adminUser, adminPass=adminPass, stopStart='Start',
                          serviceList=['RecollectApplicationData/Recollect.MapServer'])
        log.info("RecollectApplicationData/Recollect.MapServer WebService {}".format(success_msg))



        # CLEARING CACHED RUNTIME EXECUTION IN MEMORY FILES
        arcpy.Delete_management("in_memory")
        log.info("in_memory files deleted".format(success_msg))

        automated_emails.auto_email(["Jeff.Long@apexnc.org"], subject="SCRIPT COMPLETED: Recollect Data "
                                                                            "Import/Ovewrite/Dataupload"
                                                                             "Script"
                                    , text="Recollect Map Application Data Upload script "
                                           "has successfully completed "
                                           "without errors and is ready for REST service consumption")
        log.info("script completion email {}".format(success_msg))
        raise sys.exit()
    except Exception as e:
        tb = traceback.format_exc()
        email(tb)
        log.warning("{}----{}".format(e, tb))
        raise sys.exit()


if __name__ =="__main__":
    main()
