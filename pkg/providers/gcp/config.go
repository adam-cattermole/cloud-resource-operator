package gcp

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/integr8ly/cloud-resource-operator/internal/k8sutil"
	"github.com/integr8ly/cloud-resource-operator/pkg/providers"
	"github.com/integr8ly/cloud-resource-operator/pkg/resources"
	errorUtil "github.com/pkg/errors"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	controllerruntime "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	defaultReconcileTime         = time.Second * 30
	ResourceIdentifierAnnotation = "resourceIdentifier"
	DefaultFinalizer             = "cloud-resources-operator.integreatly.org/finalizers"
	DefaultConfigMapName         = "cloud-resources-gcp-strategies"
)

//DefaultConfigMapNamespace is the default namespace that Configmaps will be created in
var DefaultConfigMapNamespace, _ = k8sutil.GetWatchNamespace()

type StrategyConfig struct {
	Region      string          `json:"region"`
	ProjectID   string          `json:"projectID"`
	RawStrategy json.RawMessage `json:"strategy"`
}

type ConfigManager interface {
	ReadStorageStrategy(ctx context.Context, rt providers.ResourceType, tier string) (*StrategyConfig, error)
}

var _ ConfigManager = (*ConfigMapConfigManager)(nil)

type ConfigMapConfigManager struct {
	configMapName      string
	configMapNamespace string
	client             client.Client
}

func NewConfigMapConfigManager(cm string, namespace string, client client.Client) *ConfigMapConfigManager {
	if cm == "" {
		cm = DefaultConfigMapName
	}
	if namespace == "" {
		namespace = DefaultConfigMapNamespace
	}
	return &ConfigMapConfigManager{
		configMapName:      cm,
		configMapNamespace: namespace,
		client:             client,
	}
}

func NewDefaultConfigMapConfigManager(client client.Client) *ConfigMapConfigManager {
	return NewConfigMapConfigManager(DefaultConfigMapName, DefaultConfigMapNamespace, client)
}

func (m *ConfigMapConfigManager) ReadStorageStrategy(ctx context.Context, rt providers.ResourceType, tier string) (*StrategyConfig, error) {
	stratCfg, err := m.getTierStrategyForProvider(ctx, string(rt), tier)
	if err != nil {
		return nil, errorUtil.Wrapf(err, "failed to get tier to strategy mapping for resource type %s", string(rt))
	}
	return stratCfg, nil
}

func (m *ConfigMapConfigManager) getTierStrategyForProvider(ctx context.Context, rt string, tier string) (*StrategyConfig, error) {
	cm, err := resources.GetConfigMapOrDefault(ctx, m.client, types.NamespacedName{Name: m.configMapName, Namespace: m.configMapNamespace}, BuildDefaultConfigMap(m.configMapName, m.configMapNamespace))
	if err != nil {
		return nil, errorUtil.Wrapf(err, "failed to get gcp strategy config map %s in namespace %s", m.configMapName, m.configMapNamespace)
	}
	rawStrategyMapping := cm.Data[rt]
	if rawStrategyMapping == "" {
		return nil, errorUtil.New(fmt.Sprintf("gcp strategy for resource type %s is not defined", rt))
	}
	var strategyMapping map[string]*StrategyConfig
	if err = json.Unmarshal([]byte(rawStrategyMapping), &strategyMapping); err != nil {
		return nil, errorUtil.Wrapf(err, "failed to unmarshal strategy mapping for resource type %s", rt)
	}
	if strategyMapping[tier] == nil {
		return nil, errorUtil.New(fmt.Sprintf("no strategy found for deployment type %s and deployment tier %s", rt, tier))
	}
	return strategyMapping[tier], nil
}

func BuildDefaultConfigMap(name, namespace string) *v1.ConfigMap {
	return &v1.ConfigMap{
		ObjectMeta: controllerruntime.ObjectMeta{
			Name:      name,
			Namespace: namespace,
		},
		Data: map[string]string{
			"blobstorage": "{\"development\": { \"region\": \"\", \"projectId\": \"\", \"_network\": \"\", \"strategy\": {} }, \"production\": { \"region\": \"\", \"projectId\": \"\", \"_network\": \"\", \"strategy\": {} }}",
			"redis":       "{\"development\": { \"region\": \"\", \"projectId\": \"\", \"_network\": \"\", \"strategy\": {} }, \"production\": { \"region\": \"\", \"projectId\": \"\", \"_network\": \"\", \"strategy\": {} }}",
			"postgres":    "{\"development\": { \"region\": \"\", \"projectId\": \"\", \"_network\": \"\", \"strategy\": {} }, \"production\": { \"region\": \"\", \"projectId\": \"\", \"_network\": \"\", \"strategy\": {} }}",
			"_network":    "{\"development\": { \"region\": \"\", \"projectId\": \"\", \"_network\": \"\", \"strategy\": {} }, \"production\": { \"region\": \"\", \"projectId\": \"\", \"_network\": \"\", \"strategy\": {} }}",
		},
	}
}

func BuildInfraNameFromObject(ctx context.Context, c client.Client, om controllerruntime.ObjectMeta, n int) (string, error) {
	clusterID, err := resources.GetClusterID(ctx, c)
	if err != nil {
		return "", errorUtil.Wrap(err, "failed to retrieve cluster identifier")
	}
	return resources.ShortenString(fmt.Sprintf("%s-%s-%s", clusterID, om.Namespace, om.Name), n), nil
}

func GetRegionFromStrategyOrDefault(ctx context.Context, c client.Client, strategy *StrategyConfig) (string, error) {
	defaultRegion, err := getDefaultRegion(ctx, c)
	if err != nil {
		return "", errorUtil.Wrap(err, "failed to get default region")
	}
	region := strategy.Region
	if region == "" {
		region = defaultRegion
	}
	return region, nil
}

func getDefaultRegion(ctx context.Context, c client.Client) (string, error) {
	region, err := resources.GetGCPRegion(ctx, c)
	if err != nil {
		return "", errorUtil.Wrap(err, "failed to retrieve region from cluster")
	}
	if region == "" {
		return "", errorUtil.New("failed to retrieve region from cluster, region is not defined")
	}
	return region, nil
}

func GetProjectIdFromStrategyOrDefault(ctx context.Context, c client.Client, strategy *StrategyConfig) (string, error) {
	defaultProjectID, err := getDefaultProjectId(ctx, c)
	if err != nil {
		return "", errorUtil.Wrap(err, "failed to get default projectID")
	}
	projectID := strategy.ProjectID
	if projectID == "" {
		projectID = defaultProjectID
	}
	return projectID, nil
}

func getDefaultProjectId(ctx context.Context, c client.Client) (string, error) {
	projectID, err := resources.GetGCPProjectID(ctx, c)
	if err != nil {
		return "", errorUtil.Wrap(err, "failed to retrieve projectID from cluster")
	}
	if projectID == "" {
		return "", errorUtil.New("failed to retrieve projectID from cluster, projectID is not defined")
	}
	return projectID, nil
}
