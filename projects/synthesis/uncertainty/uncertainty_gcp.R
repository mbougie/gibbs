require(googleComputeEngineR)


project_id = 'us-agriculture-atlas'
zone = "us-central1-b"
location = "us-central1"
account_key = "C:\\Users\\Bougie\\Box\\NWF-RFS project\\rfs_uncertainty\\US Agriculture Atlas-f075dbef6518.json"


# rm(list = ls())


Sys.setenv(GCE_AUTH_FILE = account_key,
           GCE_DEFAULT_PROJECT_ID = project_id,
           GCE_DEFAULT_ZONE = zone)

options(googleAuthR.scopes.selected = "https://www.googleapis.com/auth/cloud-platform")
gce_auth()

gce_global_project(project_id) 
gce_global_zone(zone)

default_project = gce_get_project(project_id) 
default_project$name

####build the VM
vm = gce_vm(template = "rstudio", 
            name = "usxp-rfs-trial1", 
            username = "usxp-rfs", 
            password = "usxp-rfs", 
            predefined_type = "n1-highmem-2")

my_rstudio = gce_vm("usxp-rfs-trial1") 
gce_list_instances()


# job = gce_vm_stop("usxp-rfs-trial1") 
# gce_list_instances()