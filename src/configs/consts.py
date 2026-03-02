preset_permissions = {
  "system_manage": {
    "name": "系统管理",
    "description": "拥有所有的管理权限",
    "children": ["user_manage", "application_manage"]
  },
  "user_manage": {
    "name": "用户管理",
    "description": "拥有用户管理权限",
    "children": []
  },
  "application_manage": {
    "name": "应用管理",
    "description": "拥有应用管理权限",
    "children": []
  }
}