package gcp

import "time"

const (
	defaultReconcileTime         = time.Second * 30
	ResourceIdentifierAnnotation = "resourceIdentifier"
	DefaultFinalizer             = "cloud-resources-operator.integreatly.org/finalizers"
)
