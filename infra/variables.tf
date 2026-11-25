variable "region" {
  description = "AWS region. Singapore is enabled in every account; Jakarta (ap-southeast-3) must be opted into first."
  type        = string
  default     = "ap-southeast-1"
}

variable "name" {
  description = "Prefix of every resource name: buckets, function, role, workgroup, policies."
  type        = string
  default     = "quake-lake"
  validation {
    # Lowercase for bucket names; 20 characters keeps "<name>-athena-" within S3's
    # 37-character limit for a bucket_prefix.
    condition     = can(regex("^[a-z][a-z0-9-]{0,19}$", var.name))
    error_message = "Use 1 to 20 lowercase letters, digits or hyphens, starting with a letter."
  }
}

variable "plan_without_credentials" {
  description = "Plan with no AWS account or keys (see versions.tf). Never set it for an apply."
  type        = bool
  default     = false
}
