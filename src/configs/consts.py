preset_permissions = {
  "system_manage": {
    "name": "系统管理",
    "description": "拥有所有的管理权限",
    "children": [
      "user_manage",
      "application_manage",
      "application_quota_manage",
      "region_manage",
      "storage_operations_manage",
    ]
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
  "application_quota_manage": {
    "name": "应用配额管理",
    "description": "拥有应用存储配额管理权限",
    "children": []
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
  },
  "storage_operations_manage": {
    "name": "存储运维管理",
    "description": "拥有复制、集群健康及运维操作权限",
    "children": []
  }
}

ROLE_USER = "用户"
ROLE_APPLICATION_ADMIN = "应用管理员"
ROLE_OPERATIONS_ADMIN = "运维管理员"
ROLE_USER_ADMIN = "用户管理员"
ROLE_SUPERADMIN = "管理员"

system_role_definitions = {
  ROLE_USER: {
    "is_admin": False,
    "permissions": ["application_view", "region_view"],
  },
  ROLE_APPLICATION_ADMIN: {
    "is_admin": False,
    "permissions": ["application_manage", "application_quota_manage"],
  },
  ROLE_OPERATIONS_ADMIN: {
    "is_admin": False,
    "permissions": ["storage_operations_manage"],
  },
  ROLE_USER_ADMIN: {
    "is_admin": False,
    "permissions": ["user_manage"],
  },
  ROLE_SUPERADMIN: {
    "is_admin": True,
    "permissions": ["system_manage"],
  },
}

preset_admin_users = [
  "zhangle"
]
