package gcp

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"cloud.google.com/go/storage"
	"google.golang.org/api/iterator"

	"github.com/integr8ly/cloud-resource-operator/apis/integreatly/v1alpha1"
	croType "github.com/integr8ly/cloud-resource-operator/apis/integreatly/v1alpha1/types"
	"github.com/integr8ly/cloud-resource-operator/pkg/annotations"
	"github.com/integr8ly/cloud-resource-operator/pkg/providers"
	"github.com/integr8ly/cloud-resource-operator/pkg/resources"
	errorUtil "github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	blobstorageProviderName        = "gcp-bucket"
	defaultGcpBucketNameLength     = 40
	DetailsBlobStorageBucketName   = "bucketName"
	DetailsBlobStorageBucketRegion = "bucketRegion"
	DetailsBlobStorageProjectID    = "bucketProjectID"
)

type BlobStorageDeploymentDetails struct {
	BucketName      string
	BucketRegion    string
	BucketProjectID string
}

func (d *BlobStorageDeploymentDetails) Data() map[string][]byte {
	return map[string][]byte{
		DetailsBlobStorageBucketName:   []byte(d.BucketName),
		DetailsBlobStorageBucketRegion: []byte(d.BucketRegion),
		DetailsBlobStorageProjectID:    []byte(d.BucketProjectID),
	}
}

var _ providers.BlobStorageProvider = (*BlobStorageProvider)(nil)

type BlobStorageProvider struct {
	Client        client.Client
	Logger        *logrus.Entry
	ConfigManager ConfigManager
}

func NewGCPBlobStorageProvider(client client.Client, logger *logrus.Entry) *BlobStorageProvider {
	return &BlobStorageProvider{
		Client:        client,
		Logger:        logger.WithFields(logrus.Fields{"provider": blobstorageProviderName}),
		ConfigManager: NewDefaultConfigMapConfigManager(client),
	}
}

func (p *BlobStorageProvider) GetName() string {
	return blobstorageProviderName
}

func (p *BlobStorageProvider) SupportsStrategy(d string) bool {
	return d == providers.GCPDeploymentStrategy
}

func (p *BlobStorageProvider) GetReconcileTime(bs *v1alpha1.BlobStorage) time.Duration {
	if bs.Status.Phase != croType.PhaseComplete {
		return time.Second * 60
	}
	return resources.GetForcedReconcileTimeOrDefault(defaultReconcileTime)
}

func (p *BlobStorageProvider) CreateStorage(ctx context.Context, bs *v1alpha1.BlobStorage) (*providers.BlobStorageInstance, croType.StatusMessage, error) {
	// handle provider-specific finalizer
	if err := resources.CreateFinalizer(ctx, p.Client, bs, DefaultFinalizer); err != nil {
		return nil, "failed to set finalizer", err
	}

	// info about the bucket to be created
	p.Logger.Infof("getting gcp bucket config for blob storage instance %s", bs.Name)
	gcpConfig, stratCfg, err := p.buildGcpBucketConfig(ctx, bs)
	if err != nil {
		errMsg := "failed to build gcp bucket config"
		return nil, croType.StatusMessage(errMsg), errorUtil.Wrap(err, errMsg)
	}

	// TODO: build and extract credentials to use for gcp client

	gcpClient, err := storage.NewClient(ctx)
	if err != nil {
		errMsg := "failed to create gcp client to create bucket"
		return nil, croType.StatusMessage(errMsg), errorUtil.Wrap(err, errMsg)
	}

	defer gcpClient.Close()

	// create bucket if it doesn't already exist, if it does exist then use the existing bucket
	p.Logger.Infof("reconciling gcp bucket %s", gcpConfig.Name)
	msg, err := p.reconcileBucketCreate(ctx, bs, gcpClient, gcpConfig, stratCfg)
	if err != nil {
		return nil, msg, errorUtil.Wrapf(err, string(msg))
	}

	// Adding labels to gcp
	msg, err = p.TagBlobStorage(ctx, bs, gcpClient, gcpConfig)
	if err != nil {
		errMsg := fmt.Sprintf("failed to add labels to bucket: %s", msg)
		return nil, croType.StatusMessage(errMsg), errorUtil.Wrap(err, errMsg)
	}

	bsi := &providers.BlobStorageInstance{
		DeploymentDetails: &BlobStorageDeploymentDetails{
			BucketName:      gcpConfig.Name,
			BucketRegion:    stratCfg.Region,
			BucketProjectID: stratCfg.ProjectID,
		},
	}

	p.Logger.Infof("creation handler for blob storage instance %s in namespace %s finished successfully", bs.Name, bs.Namespace)
	return bsi, msg, nil
}

func (p *BlobStorageProvider) reconcileBucketCreate(ctx context.Context, bs *v1alpha1.BlobStorage, gcpClient *storage.Client, gcpConfig *storage.BucketAttrs, stratConfig *StrategyConfig) (croType.StatusMessage, error) {
	// create bucket handle
	bkt := gcpClient.Bucket(gcpConfig.Name)

	// check if bucket already exists in gcp
	attrs, err := bkt.Attrs(ctx)
	if err != nil && err != storage.ErrBucketNotExist {
		return "failed to retrieve bucket", err
	}

	if attrs != nil {
		// TODO: bucket found, reconcile settings
		// bkt.Update(ctx, storage.BucketAttrsToUpdate{})

		msg := fmt.Sprintf("using bucket %s", attrs.Name)
		return croType.StatusMessage(msg), nil
	}

	// attrs == nil at this point, if CR has resourceIdentifier
	// annotation it should be present. We shouldn't create it again,
	// and manual intervention is required to restore from backup
	if annotations.Has(bs, ResourceIdentifierAnnotation) {
		errMsg := fmt.Sprintf("BlobStorage CR %s in %s namespace has %s annotation with value %s, but no corresponding GCP Bucket was found",
			bs.Name, bs.Namespace, ResourceIdentifierAnnotation, bs.ObjectMeta.Annotations[ResourceIdentifierAnnotation])
		return croType.StatusMessage(errMsg), fmt.Errorf(errMsg)
	}

	// create bucket
	p.Logger.Infof("bucket %s not found, creating bucket", gcpConfig.Name)
	if err = bkt.Create(ctx, stratConfig.ProjectID, nil); err != nil {
		errMsg := fmt.Sprintf("failed to create gcp bucket %s", gcpConfig.Name)
		return croType.StatusMessage(errMsg), errorUtil.Wrapf(err, errMsg)
	}

	annotations.Add(bs, ResourceIdentifierAnnotation, gcpConfig.Name)
	if err := p.Client.Update(ctx, bs); err != nil {
		errMsg := "failed to add annotation"
		return croType.StatusMessage(errMsg), errorUtil.Wrapf(err, errMsg)
	}

	// TODO: reconcile bucket settings
	// bkt.Update(ctx, storage.BucketAttrsToUpdate{})

	p.Logger.Infof("reconcile for gcp bucket completed successfully")
	return "successfully reconciled", nil
}

func (p *BlobStorageProvider) TagBlobStorage(ctx context.Context, bs *v1alpha1.BlobStorage, gcpClient *storage.Client, gcpConfig *storage.BucketAttrs) (croType.StatusMessage, error) {
	p.Logger.Infof("bucket %s found, Adding labels to bucket", gcpConfig.Name)

	// set tag values that will always be added
	defaultOrganizationTag := resources.GetOrganizationTagGcp()

	clusterID, err := resources.GetClusterID(ctx, p.Client)
	if err != nil {
		errMsg := "failed to get cluster id"
		return croType.StatusMessage(errMsg), errorUtil.Wrapf(err, errMsg)
	}

	attrs := &storage.BucketAttrsToUpdate{}
	attrs.SetLabel(defaultOrganizationTag+"clusterid", clusterID)
	attrs.SetLabel(defaultOrganizationTag+"resource-type", bs.Spec.Type)
	attrs.SetLabel(defaultOrganizationTag+"resource-name", bs.Name)

	if bs.ObjectMeta.Labels["productName"] != "" {
		attrs.SetLabel(defaultOrganizationTag+"product-name", strings.ToLower(bs.ObjectMeta.Labels["productName"]))
	}

	_, err = gcpClient.Bucket(gcpConfig.Name).Update(ctx, *attrs)
	if err != nil {
		errMsg := fmt.Sprintf("failed to add labels to gcp bucket: %s", err)
		return croType.StatusMessage(errMsg), errorUtil.Wrapf(err, errMsg)
	}

	logrus.Infof("successfully created or updated labels on gcp bucket %s", gcpConfig.Name)
	return "successfully created and tagged", nil
}

func (p *BlobStorageProvider) DeleteStorage(ctx context.Context, bs *v1alpha1.BlobStorage) (croType.StatusMessage, error) {
	p.Logger.Infof("deleting blob storage instance %s via gcp", bs.Name)

	// resolve bucket information for bucket created by provider
	p.Logger.Infof("getting gcp bucket config for blob storage instance %s", bs.Name)
	gcpConfig, stratCfg, err := p.buildGcpBucketConfig(ctx, bs)
	if err != nil {
		errMsg := "failed to build gcp bucket config"
		return croType.StatusMessage(errMsg), errorUtil.Wrap(err, errMsg)
	}

	// TODO: get credentials for client for deletion

	gcpClient, err := storage.NewClient(ctx)
	if err != nil {
		errMsg := "failed to create gcp client to delete bucket"
		return croType.StatusMessage(errMsg), errorUtil.Wrap(err, errMsg)
	}

	defer gcpClient.Close()

	return p.reconcileBucketDelete(ctx, bs, gcpClient, gcpConfig, stratCfg)
}

func (p *BlobStorageProvider) reconcileBucketDelete(ctx context.Context, bs *v1alpha1.BlobStorage, gcpClient *storage.Client, gcpConfig *storage.BucketAttrs, stratCfg *StrategyConfig) (croType.StatusMessage, error) {
	// create bucket handle
	bkt := gcpClient.Bucket(gcpConfig.Name)

	// check if bucket has already been deleted
	attrs, err := bkt.Attrs(ctx)
	if err != nil && err != storage.ErrBucketNotExist {
		return "failed to retrieve bucket", err
	}

	if attrs == nil {
		resources.RemoveFinalizer(&bs.ObjectMeta, DefaultFinalizer)
		if err = p.Client.Update(ctx, bs); err != nil {
			return croType.StatusMessage("failed to update blob storage cr as part of finalizer reconcile"), err
		}
		return croType.StatusEmpty, nil
	}

	_, err = getBucketSize(ctx, bkt)
	if err != nil {
		errMsg := fmt.Sprintf("unable to get bucket size : %s", gcpConfig.Name)
		return croType.StatusMessage(errMsg), errorUtil.Wrapf(err, errMsg)
	}
	//TODO: custom check for force bucket deletion, for now we just
	//clear all objects anyway - unsafe care - use bucket size above

	if err := emptyBucket(ctx, bkt); err != nil {
		errMsg := fmt.Sprintf("unable to empty bucket : %s", gcpConfig.Name)
		return croType.StatusMessage(errMsg), errorUtil.Wrapf(err, errMsg)
	}
	err = bkt.Delete(ctx)
	if err != nil && err != storage.ErrBucketNotExist {
		errMsg := fmt.Sprintf("failed to delete gcp bucket: %s", err)
		return croType.StatusMessage(errMsg), errorUtil.Wrapf(err, errMsg)
	}

	return croType.StatusEmpty, nil
}

func (p *BlobStorageProvider) buildGcpBucketConfig(ctx context.Context, bs *v1alpha1.BlobStorage) (*storage.BucketAttrs, *StrategyConfig, error) {
	// info about the bucket to be created
	p.Logger.Infof("getting gcp bucket config for blob storage instance %s", bs.Name)
	gcpConfig, stratCfg, err := p.getGcpBucketConfig(ctx, bs)
	if err != nil {
		return nil, nil, errorUtil.Wrapf(err, fmt.Sprintf("failed to retrieve gcp bucket config for blob storage instance %s", bs.Name))
	}

	// cluster infra info
	p.Logger.Info("getting cluster id from infrastructure for bucket naming")
	bucketName, err := BuildInfraNameFromObject(ctx, p.Client, bs.ObjectMeta, defaultGcpBucketNameLength)
	if err != nil {
		return nil, nil, errorUtil.Wrapf(err, fmt.Sprintf("failed to retrieve gcp bucket config for blob storage instance %s", bs.Name))
	}
	if gcpConfig.Name == "" {
		gcpConfig.Name = bucketName
	}

	return gcpConfig, stratCfg, nil
}

func (p *BlobStorageProvider) getGcpBucketConfig(ctx context.Context, bs *v1alpha1.BlobStorage) (*storage.BucketAttrs, *StrategyConfig, error) {
	stratCfg, err := p.ConfigManager.ReadStorageStrategy(ctx, providers.BlobStorageResourceType, bs.Spec.Tier)
	if err != nil {
		return nil, nil, errorUtil.Wrap(err, "failed to read aws strategy config")
	}

	defRegion, err := GetRegionFromStrategyOrDefault(ctx, p.Client, stratCfg)
	if err != nil {
		return nil, nil, errorUtil.Wrap(err, "failed to get default region")
	}
	if stratCfg.Region == "" {
		p.Logger.Debugf("region not set in deployment strategy configuration, using default region %s", defRegion)
		stratCfg.Region = defRegion
	}

	defProjectID, err := GetProjectIdFromStrategyOrDefault(ctx, p.Client, stratCfg)
	if err != nil {
		return nil, nil, errorUtil.Wrap(err, "failed to get default projectID")
	}
	if stratCfg.ProjectID == "" {
		p.Logger.Debugf("projectID not set in deployment strategy configuration, using default projectID %s", defProjectID)
		stratCfg.ProjectID = defProjectID
	}

	gcpConfig := &storage.BucketAttrs{}
	if err = json.Unmarshal(stratCfg.RawStrategy, gcpConfig); err != nil {
		return nil, nil, errorUtil.Wrapf(err, "failed to unmarshal gcp strategy configuration")
	}

	return gcpConfig, stratCfg, nil
}

func getBucketSize(ctx context.Context, bkt *storage.BucketHandle) (int, error) {
	i := 0
	objects := bkt.Objects(ctx, nil)
	for {
		_, err := objects.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			errMsg := "unable to list items in bucket"
			return 0, errorUtil.Wrapf(err, errMsg)
		}
		i++
	}
	return i, nil
}

func emptyBucket(ctx context.Context, bkt *storage.BucketHandle) error {
	size, err := getBucketSize(ctx, bkt)
	if err != nil {
		return err
	}

	if size == 0 {
		return nil
	}

	objects := bkt.Objects(ctx, nil)
	for {
		attr, err := objects.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			errMsg := "unable to list objects in bucket"
			return errorUtil.Wrapf(err, errMsg)
		}
		obj := bkt.Object(attr.Name)
		if err = obj.Delete(ctx); err != nil {
			errMsg := "unable to delete objects from bucket"
			return errorUtil.Wrapf(err, errMsg)
		}
	}

	return nil
}
