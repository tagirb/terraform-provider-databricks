package access_test

import (
	"testing"

	"github.com/databricks/terraform-provider-databricks/internal/acceptance"
)

func TestAccPermissionAssignmentPrincipalId(t *testing.T) {
	acceptance.WorkspaceLevel(t, acceptance.Step{
		Template: `
		resource "databricks_group" "this" {
			display_name = "TF {var.RANDOM}"
		}
		resource "databricks_permission_assignment" "this" {
			principal_id = databricks_group.this.id
			permissions  = ["USER"]
		}`,
	}, acceptance.Step{
		Template: `
		resource "databricks_group" "this" {
			display_name = "TF {var.RANDOM}"
		}
		resource "databricks_permission_assignment" "this" {
			principal_id = databricks_group.this.id
			permissions  = ["ADMIN"]
		}`,
	})
}

func TestAccPermissionAssignmentGroupName(t *testing.T) {
	acceptance.WorkspaceLevel(t, acceptance.Step{
		Template: `
		resource "databricks_group" "this" {
			display_name = "TF {var.RANDOM}"
		}
		resource "databricks_permission_assignment" "this" {
			group_name  = databricks_group.this.display_name
			permissions = ["USER"]
		}`,
	})
}
