package mws

import (
	"context"
	"fmt"
	"sync"

	"github.com/databricks/databricks-sdk-go/apierr"
	"github.com/databricks/databricks-sdk-go/service/iam"
	"github.com/databricks/terraform-provider-databricks/common"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
	"golang.org/x/sync/singleflight"
)

// mwsAssignmentsKey uniquely identifies a cached permission assignment list
// by the account client pointer and workspace ID.
type mwsAssignmentsKey struct {
	acc         any
	workspaceId int64
}

// mwsPermissionAssignmentsCache caches workspace permission assignment lists.
// The cache is keyed by mwsAssignmentsKey so concurrent reads for the same
// workspace share a single result. mwsPermissionAssignmentsSF ensures that
// when many goroutines all miss the cache simultaneously, only one in-flight
// API call is made; the rest block and share the result.
var (
	mwsPermissionAssignmentsCache sync.Map
	mwsPermissionAssignmentsSF    singleflight.Group
)

func getPermissionsByPrincipal(list iam.PermissionAssignments, principalId int64) (res iam.UpdateWorkspaceAssignments, err error) {
	for _, v := range list.PermissionAssignments {
		if v.Principal.PrincipalId != principalId {
			continue
		}
		return iam.UpdateWorkspaceAssignments{Permissions: v.Permissions}, nil
	}
	return res, &apierr.APIError{
		ErrorCode:  "NOT_FOUND",
		StatusCode: 404,
		Message:    fmt.Sprintf("%d not found", principalId),
	}
}

func ResourceMwsPermissionAssignment() common.Resource {
	s := common.StructToSchema(iam.UpdateWorkspaceAssignments{},
		func(m map[string]*schema.Schema) map[string]*schema.Schema {
			common.CustomizeSchemaPath(m).AddNewField("workspace_id", &schema.Schema{
				Type:     schema.TypeInt,
				Required: true,
			}).AddNewField("principal_id", &schema.Schema{
				Type:     schema.TypeInt,
				Required: true,
			})
			common.CustomizeSchemaPath(m, "permissions").SetRequired().SetSliceSet()
			return m
		})
	pair := common.NewPairID("workspace_id", "principal_id").Schema(
		func(m map[string]*schema.Schema) map[string]*schema.Schema {
			return s
		})
	return common.Resource{
		Schema: s,
		Create: func(ctx context.Context, d *schema.ResourceData, c *common.DatabricksClient) error {
			acc, err := c.AccountClient()
			if err != nil {
				return err
			}
			var assignment iam.UpdateWorkspaceAssignments
			common.DataToStructPointer(d, s, &assignment)
			assignment.PrincipalId = common.GetInt64(d, "principal_id")
			assignment.WorkspaceId = common.GetInt64(d, "workspace_id")
			_, err = acc.WorkspaceAssignment.Update(ctx, assignment)
			if err != nil {
				return err
			}
			pair.Pack(d)
			mwsPermissionAssignmentsCache.Delete(mwsAssignmentsKey{acc: acc, workspaceId: assignment.WorkspaceId})
			return nil
		},
		Read: func(ctx context.Context, d *schema.ResourceData, c *common.DatabricksClient) error {
			acc, err := c.AccountClient()
			if err != nil {
				return err
			}
			workspaceId, principalId, err := pair.Unpack(d)
			if err != nil {
				return fmt.Errorf("parse id: %w", err)
			}
			key := mwsAssignmentsKey{acc: acc, workspaceId: common.MustInt64(workspaceId)}
			var list *iam.PermissionAssignments
			if cached, ok := mwsPermissionAssignmentsCache.Load(key); ok {
				list = cached.(*iam.PermissionAssignments)
			} else {
				sfKey := fmt.Sprintf("%p/%d", acc, common.MustInt64(workspaceId))
				v, sfErr, _ := mwsPermissionAssignmentsSF.Do(sfKey, func() (any, error) {
					result, e := acc.WorkspaceAssignment.ListByWorkspaceId(ctx, common.MustInt64(workspaceId))
					if e == nil {
						mwsPermissionAssignmentsCache.Store(key, result)
					}
					return result, e
				})
				if sfErr != nil {
					return sfErr
				}
				list = v.(*iam.PermissionAssignments)
			}
			permissions, err := getPermissionsByPrincipal(*list, common.MustInt64(principalId))
			if err != nil {
				return err
			}
			d.Set("workspace_id", common.MustInt64(workspaceId))
			d.Set("principal_id", common.MustInt64(principalId))
			return common.StructToData(permissions, s, d)
		},
		Delete: func(ctx context.Context, d *schema.ResourceData, c *common.DatabricksClient) error {
			acc, err := c.AccountClient()
			if err != nil {
				return err
			}
			workspaceId, principalId, err := pair.Unpack(d)
			if err != nil {
				return fmt.Errorf("parse id: %w", err)
			}
			err = acc.WorkspaceAssignment.DeleteByWorkspaceIdAndPrincipalId(ctx, common.MustInt64(workspaceId), common.MustInt64(principalId))
			if err != nil {
				return err
			}
			mwsPermissionAssignmentsCache.Delete(mwsAssignmentsKey{acc: acc, workspaceId: common.MustInt64(workspaceId)})
			return nil
		},
	}
}
