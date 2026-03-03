preset_permissions = {
  "system_manage": {
    "name": "系统管理",
    "description": "拥有所有的管理权限",
    "children": ["user_manage", "application_manage", "region_manage"]
  },
  "user_view": {
    "name": "用户查看",
    "description": "拥有用户查看权限",
    "children": []
  },
  "user_manage": {
    "name": "用户管理",
    "description": "拥有用户管理权限",
    "children": ["user_view", "role_manage"]
  },
  "role_view": {
    "name": "角色查看",
    "description": "拥有角色查看权限",
    "children": []
  },
  "role_manage": {
    "name": "角色管理",
    "description": "拥有角色管理权限",
    "children": ["role_view"]
  },
  "application_view": {
    "name": "应用查看",
    "description": "拥有应用查看权限",
    "children": []
  },
  "application_manage": {
    "name": "应用管理",
    "description": "拥有应用管理权限",
    "children": ["application_view"]
  },
  "region_view": {
    "name": "区域查看",
    "description": "拥有区域查看权限",
    "children": []
  },
  "region_manage": {
    "name": "区域管理",
    "description": "拥有区域管理权限",
    "children": ["region_view"]
  }
}