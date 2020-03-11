"""
Script: Overwrite Publish Recollect web services
Purpose: overwrite feature services for yard waste , trash, recycling, and assigned neighborhood days. Runs Twice a week Wednesdays and Sundays
Author: Jeff Long
"""


import arcpy, os, sys, smtplib
from arcgis.gis import GIS
import traceback


def auto_email(TO, subject,text):
    FROM = "Jeff.Long@apexnc.org"
    SERVER = "#"
    header_mesg = "---THIS IS A PYTHON SCRIPT GENERATED EMAIL---"
    footer = "Thank You,\n\n \n Jeff Long " \
             "\n Information Technology  " \
             " \n GIS Administrator " \
             "\n P: 241-3409-7515 "
    message = """From: {0}\r\nTo: {1}\r\nSubject: {2}\r\n\

    {3}\r\n\n\n {4}\r\n\n\n {5}""" .format(FROM, ", ".join(TO), subject, header_mesg, text, footer)

    server = smtplib.SMTP(SERVER)
    server.sendmail(FROM, TO, message)
    server.quit()


arcpy.env.overwriteOutput= 1


arcpy.SignInToPortal('https://www.arcgis.com', 'jlong_apexnc', '#')

projdoc = r"C:\Users\Jlong\Documents\ArcGIS\Projects\RecollectPublishServicesLGDB\RecollectPublishServicesLGDB.aprx"
direct = r"C:\Users\Jlong\Documents\ArcGIS\Projects\RecollectPublishServicesLGDB"


def email(x): auto_email(["Jeff.Long@apexnc.org"], subject="ERROR DETECTED: Recollect Data "
                                                                            "Import/Ovewrite/Dataupload"
                                                                            "Script", text=str(x))


def create_service_definition(map_proj, sname, mpname, proj_dir, weblyrname):
    agol_serv_con = 'My Hosted Services'
    aprx = arcpy.mp.ArcGISProject(map_proj)
    outServiceDefinition = os.path.join(proj_dir, "{}.sd".format(sname))

    sddraft_output_filename = os.path.join(proj_dir, "{}.sddraft".format(sname))
    try:
        mplyrs = aprx.listMaps(mpname)[0]
        #print (mplyrs)
        arcpy.mp.CreateWebLayerSDDraft(mplyrs, sddraft_output_filename, weblyrname, 'MY_HOSTED_SERVICES',
                                       'FEATURE_ACCESS', overwrite_existing_service=1)
    except:
        tb = traceback.format_exc()
        email(tb)
        raise sys.exit()

    try:
        arcpy.StageService_server(sddraft_output_filename, outServiceDefinition)
    except:
        tb = traceback.format_exc()
        email(tb)
        raise sys.exit()

    try:
        print("Uploading {} Services to AGOL".format(sname))
        arcpy.UploadServiceDefinition_server(outServiceDefinition, agol_serv_con, in_override="OVERRIDE_DEFINITION", in_public="PUBLIC",
                                             in_organization="SHARE_ORGANIZATION",
                                             in_groups=["Apex Recollect Data and Applications"],
                                             in_my_contents="SHARE_ONLINE")

    except:
        tb = traceback.format_exc()
        email(tb)
        raise sys.exit()

    print("-------Web Service Succesfully Published--------")


try:
    create_service_definition(map_proj=projdoc, sname="RecollectLocalGDB", mpname="Map", proj_dir=direct,
                              weblyrname="RecollectLocalGDB")
except:
    tb = traceback.format_exc()
    email(tb)
    raise sys.exit()

auto_email(["Jeff.Long@apexnc.org","kerrin.cox@apexnc.org","stacie.galloway@apexnc.org"], subject="SCRIPT COMPLETED: "
                                                                                           "Overwrite Yardwaste Recollect "
                                                               "Service "
                                    , text="Recollect Yardwaste Data Service Upload script "
                                           "has successfully completed "
                                           "without errors and is ready for REST service consumption")