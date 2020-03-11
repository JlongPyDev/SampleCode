"""
Script: Nightly Wake County Data Download
Purpose: Download wake county boundary and planning data for import to Apex SDE databases
Author: Jeff Long
"""

try:
    import os, zipfile, re, timeit, sys, getpass, datetime, arcpy, logging, glob, arceditor
    from arcgis.gis import GIS
except Exception as e:
    print(e)
    input('Press Enter')

arcpy.env.workspace = r"\\Apexgis\GIS\ApexSDE\current"
arcpy.env.overwriteOutput = 1
arcpy.Delete_management('in_memory')

# Clear users logged in

admin_workspace = r'C:\Users\Jlong\AppData\Roaming\ESRI\Desktop10.5\ArcCatalog\APEXWCD_ADMIN.sde'

users = arcpy.ListUsers(admin_workspace)
for user in users:
    print("Username: {0}, Connected at: {1},{2}".format(
        user.Name, user.ConnectionTime, user.ID))
    if user.Name == 'WCD':
        print ("Disconnecting----{}".format(user.Name))
        arcpy.DisconnectUser(admin_workspace, user.ID)
    if user.Name == 'WCDVIEWER':
        print("Disconnecting----{}".format(user.Name))
        arcpy.DisconnectUser(admin_workspace, user.ID)


def clearWSLocks(inputWS):
  '''Attempts to clear locks on a workspace, returns stupid message.'''
  if all([arcpy.Exists(inputWS), arcpy.Compact_management(inputWS), arcpy.Exists(inputWS)]):
    return 'Workspace (%s) clear to continue...' % inputWS
  else:
    return '!!!!!!!! ERROR WITH WORKSPACE %s !!!!!!!!' % inputWS


def init_logging(workspace, fname):

    initials = getpass.getuser()[:2].upper()

    now = datetime.datetime.now().strftime('%Y%m%d_%H_Hours_%M_Mins_%S_Sec')

    log = logging.getLogger("APPLICATION_NAME")
    log.handlers = []

    log.setLevel(logging.INFO)

    file_name = '{}_{}_{}_ValidationLog.log'.format(fname, initials, now)
    file_handler = logging.FileHandler(os.path.join(workspace, file_name), mode="w")

    log_format = '%(asctime)s %(levelname)s %(message)s'
    file_formatter = logging.Formatter(log_format, datefmt='%m/%d/%Y %I:%M:%S %p')

    file_handler.setFormatter(file_formatter)
    file_handler.setLevel(logging.INFO)
    log.addHandler(file_handler)

    return log


def zipFileGeodatabase(inFileGeodatabase, newZipFN):
   if not (os.path.exists(inFileGeodatabase)):
      return False

   if (os.path.exists(newZipFN)):
      os.remove(newZipFN)

   zipobj = zipfile.ZipFile(newZipFN,'w')

   for infile in glob.glob(inFileGeodatabase+"/*"):
      zipobj.write(infile, os.path.basename(inFileGeodatabase)+"/"+os.path.basename(infile), zipfile.ZIP_DEFLATED)
      print ("Zipping: "+infile)

   zipobj.close()

   return True


def list_fcs(input_gdb):
    arcpy.env.workspace = input_gdb
    #gdb_work = arcpy.env.workspace
    #arcpy.Compact_management(gdb_work)
    datasets = arcpy.ListDatasets(feature_type='feature')
    datasets = [''] + datasets if datasets is not None else []
    for ds in datasets:
        in_features = [str(os.path.join(input_gdb, fc)) for fc in arcpy.ListFeatureClasses(feature_dataset=ds)]

        return in_features


def import_fc_to_sde(fc):
    print(" Processing Import --- %s " % fc)
    log.info("Processing %s for import to APEXWCD SDE Database" % fc)
    sde_fc = sde_fc_format.format(os.path.basename(fc))
    subdivs = r'C:\Users\Jlong\AppData\Roaming\ESRI\Desktop10.5\ArcCatalog\APEXPL.sde\APEXPL.PL.Subdivisions\APEXPL' \
              r'.PL.ApexDevelopment_Residential'
    #arcpy.DeleteField_management(fc, ["OBJECTID"])
    if os.path.basename(fc) == 'Parcels':
        log.info("Processing {} for import to APEXWCD SDE Database".format(sde_fc_format.format('WC_ParcelApxClipped')))
        arcpy.MakeFeatureLayer_management(fc, 'wake_lyr')
        arcpy.SelectLayerByLocation_management('wake_lyr', 'INTERSECT', exp_clipper)
        arcpy.Select_analysis('wake_lyr', sde_fc_format.format('WC_ParcelApxClipped'))
        arcpy.SelectLayerByAttribute_management('wake_lyr', "CLEAR_SELECTION")
        arcpy.Delete_management("wake_lyr")
        log.info("Copied {} to APEXWCD SDE Database".format(sde_fc_format.format('WC_ParcelApxClipped')))
    elif os.path.basename(fc) == 'Jurisdictions':
        log.info("Processing %s with CURRENT Date FIELD" % sde_fc_format.format('Jurisdictions'))
        format_time = datetime.date.fromtimestamp(os.path.getmtime(fgdb))
        arcpy.AddField_management(fc, "CURRENT", 'DATE', field_length=64)
        with arcpy.da.UpdateCursor(fc, ["CURRENT"]) as ucur:
            for row in ucur:
                row[0] = format_time
                ucur.updateRow(row)

    arcpy.CopyFeatures_management(fc, sde_fc)
    log.info("Copied {} to APEXWCD SDE Database".format(sde_fc))
    return

initials = getpass.getuser()[:2].upper()
log = init_logging(r"\\Apexgis\GIS\ApexSDE\logs\APX_WCD", 'WakeCountyData')
log.info("Initializing Log:%s" % log)

exp_clipper = r'C:\Users\Jlong\AppData\Roaming\ESRI\Desktop10.5\ArcCatalog\APEXWCD.sde\APEXWCD.WCD.WakeCountyApex' \
              r'\APEXWCD.WCD.apex_clipper'

sde_connect = r'C:\Users\Jlong\AppData\Roaming\ESRI\Desktop10.5\ArcCatalog\APEXWCD.sde'

sde_fc_format = r'C:\Users\Jlong\AppData\Roaming\ESRI\Desktop10.5\ArcCatalog\APEXWCD.sde\APEXWCD.WCD' \
                r'.WakeCountyOriginal\APEXWCD.WCD.{}'

if __name__ == '__main__':

    start = timeit.default_timer()
    now = datetime.datetime.now().strftime('%Y%m%d')

    log.info("Starting Archive of Current SDE GDB State")
    apxwcd = r'C:\Users\Jlong\AppData\Roaming\ESRI\Desktop10.5\ArcCatalog\APEXWCD.sde'

    log.info("Archive Process Complete")

    # Wake DL
    log.info("Starting Wake Count Open Data FGDB Download Process")

    portal = "http://www.arcgis.com"
    user = "jlong_apexnc"  # AGOL username
    password = "#"  # AGOL password
    itemid = "0d0e011c0fcb4d07afe7da39124013e8"  # Change the item id to download other files

    download_path = r"\\Apexgis\GIS\ApexSDE\current"  # Folder you want to download the zipped FGDB to

    gis = GIS(portal, user, password)

    try:
        gis = GIS(portal, user, password)

        fgdb_item = gis.content.search("title: WakeData.gdb", item_type="File Geodatabase",outside_org=True)[0]

        log.info("Found FGDB: {}, ID: {}".format(fgdb_item.title, fgdb_item.id))
        try:
            fgdb_item.download(save_path=download_path)
            log.info("Download from Wake County Group successful")
        except Exception as e:
            log.warning("Could not download FGDB")
            log.warning(e)
    except Exception as e:
        log.warning("Could not find FGDB")
        log.warning(e)

    zip_file = zipfile.ZipFile(os.path.join(download_path, "WakeData.gdb.zip"), 'r')
    if zip_file:
        zip_file.testzip()
        zip_file.extractall(download_path)
        zip_file.close()
    else:
        log.warning("Could not Extract Zipfile")

    fgdb = r"\\Apexgis\GIS\ApexSDE\current\WakeData.gdb"

    #form_gdb = os.path.join(download_path, "WakeData_{}.gdb".format(now))
    #arcpy.Copy_management(fgdb, form_gdb)

    fcs_list = [fc for fc in list_fcs(fgdb)]
    for fc in fcs_list:
        import_fc_to_sde(fc)

    apexwcd_sde= r'C:\Users\Jlong\AppData\Roaming\ESRI\Desktop10.5\ArcCatalog\APEXWCD.sde'

    def update_dl_log(sde_gdb):

        name = "APEXWCD.WCD.WkDownloadLog"
        arcpy.env.workspace = sde_gdb
        logtable = os.path.join(sde_gdb, name)

        fields = [f.name for f in arcpy.ListFields(logtable) if f.name not in ['OBJECTID']]

        edit = arcpy.da.Editor(sde_gdb)
        edit.startEditing(False, True)
        edit.startOperation()

        icur = arcpy.da.InsertCursor(logtable,fields)

        date = now[4:6] + "/" + now[6:8] + "/" + now[0:4]
        data = [date, initials, runtime]

        row = tuple(data)
        icur.insertRow((date, initials, runtime, 'YES', 'YES', 'YES', 'YES', 'YES', 'YES', 'YES', 'YES'))

        edit.stopOperation()
        edit.stopEditing(True)
        del icur


    log.info("Creating APEXWCD SDE Table Log")
    log.info("Wake County Open Data Download Script Complete")

    stop = timeit.default_timer()
    total_time = stop - start

    mins, secs = divmod(total_time, 60)
    hours, mins = divmod(mins, 60)
    runtime = "%d:%d:%d" % (hours, mins, secs)
    runtime_msg ="Total running time: %d:%d:%d\n" % (hours, mins, secs)
    print(runtime_msg)
    log.info(runtime_msg)

    update_dl_log(apexwcd_sde)

    print ("SCRIPT ENDS")


