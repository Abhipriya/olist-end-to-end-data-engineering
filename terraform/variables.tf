variable "snowflake_organization" {
  description = "Snowflake organization"
  type        = string
  sensitive   = true
}

variable "snowflake_account" {
  description = "Snowflake account"
  type        = string
  sensitive   = true
}

variable "snowflake_user" {
  description = "Snowflake user"
  type        = string
}

variable "snowflake_private_key_path" {
  description = "Path to RSA private key"
  type        = string
  sensitive   = true
}

variable "snowflake_role" {
  description = "Snowflake role"
  type        = string
  default     = "ACCOUNTADMIN"
}