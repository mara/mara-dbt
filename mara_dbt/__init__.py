"""Make the functionalities of this package auto-discoverable by mara-app"""


def MARA_CONFIG_MODULES():
    from . import config
    return [config]


#def MARA_FLASK_BLUEPRINTS():
#    from . import views
#    return [views.blueprint]


#def MARA_ACL_RESOURCES():
#    from . import views
#    return {'DB Schema': views.acl_resource}


def MARA_CLICK_COMMANDS():
    from . import cli
    return [cli.setup]


#def MARA_NAVIGATION_ENTRIES():
#    from . import views
#    return {'DB Schema': views.navigation_entry()}
