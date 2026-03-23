import bcrypt

# 密码哈希轮数, 用于 bcrypt 加密, 数值应该介于 4 到 31 之间
BCRYPT_ROUNDS = 4

salt = bcrypt.gensalt(rounds=BCRYPT_ROUNDS)
print(salt)