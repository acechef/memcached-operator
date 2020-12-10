/*


Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package controllers

import (
	"context"
	"reflect"
	"unsafe"

	"github.com/go-logr/logr"
	"gopkg.in/yaml.v2"
	batchv1 "k8s.io/api/batch/v1"
	"k8s.io/api/batch/v1beta1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/predicate"

	cachev1alpha1 "github.com/strong-ge/memcached-operator/api/v1alpha1"
)

// MemcachedReconciler reconciles a Memcached object
type MemcachedReconciler struct {
	client.Client
	Log    logr.Logger
	Scheme *runtime.Scheme
}

// +kubebuilder:rbac:groups=cache.strong-ge.com,resources=memcacheds,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=cache.strong-ge.com,resources=memcacheds/status,verbs=get;update;patch

func (r *MemcachedReconciler) Reconcile(req ctrl.Request) (ctrl.Result, error) {

	ctx := context.Background()
	log := r.Log.WithValues("memcached", req.NamespacedName)

	// your logic here
	log.Info("进入循环了。。。。")

	memcached := &cachev1alpha1.Memcached{}
	err := r.Get(ctx, req.NamespacedName, memcached)
	if err != nil {
		if errors.IsNotFound(err) {
			// Request object not found, could have been deleted after reconcile request.
			// Owned objects are automatically garbage collected. For additional cleanup logic use finalizers.
			// Return and don't requeue
			log.Info("Memcached resource not found. Ignoring since object must be deleted")
			return ctrl.Result{}, nil
		}
		// Error reading the object - requeue the request.
		log.Error(err, "Failed to get Memcached")
		return ctrl.Result{}, err
	}

	found := &v1beta1.CronJob{}
	err = r.Get(ctx, types.NamespacedName{Name: "my-cron", Namespace: memcached.Namespace}, found)
	if err != nil && errors.IsNotFound(err) {

		cronJob := &v1beta1.CronJob{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "my-cron",
				Namespace: memcached.Namespace,
			},
			Spec: v1beta1.CronJobSpec{
				Schedule:          "*/2 * * * *",
				ConcurrencyPolicy: v1beta1.ForbidConcurrent,
				JobTemplate: v1beta1.JobTemplateSpec{
					Spec: batchv1.JobSpec{
						Template: corev1.PodTemplateSpec{
							Spec: corev1.PodSpec{
								RestartPolicy: "Never",
								Containers: []corev1.Container{
									{
										Name:  "hello",
										Image: "busybox",
										Args:  []string{"/bin/sh", "-c", "date; echo Hello from the Kubernetes cluster"},
									},
								},
							},
						},
					},
				},
			},
		}

		_ = ctrl.SetControllerReference(memcached, cronJob, r.Scheme)

		log.Info("Creating a new CronJob", "CronJob.Namespace", cronJob.Namespace, "CronJob.Name", cronJob.Name)
		err = r.Create(ctx, cronJob)

		if err != nil {
			log.Error(err, "Failed to create new Deployment", "Deployment.Namespace", cronJob.Namespace, "Deployment.Name", cronJob.Name)
			return ctrl.Result{}, err
		}
		// Deployment created successfully - return and requeue
		//return ctrl.Result{Requeue: true}, nil
		return ctrl.Result{}, nil
	} else if err != nil {
		log.Error(err, "Failed to get Deployment")
		return ctrl.Result{}, err
	}

	log.Info("这边是更新的流程")

	cronjobList := &v1beta1.CronJobList{}
	if err := r.List(context.TODO(), cronjobList); err != nil {
		return ctrl.Result{}, err
	}
	log.Info("查询cronjob列表...")
	for _, cronjob := range cronjobList.Items {
		if len(cronjob.Status.Active) > 0 {
			log.Info("job名字是:" + cronjob.Status.Active[0].Name)
			//创建configmap
			jr := JobResult{Status: "active"}
			b, _ := yaml.Marshal(jr)
			cm := &corev1.ConfigMap{
				ObjectMeta: metav1.ObjectMeta{
					Name:      cronjob.Status.Active[0].Name,
					Namespace: memcached.Namespace,
				},
				Data: map[string]string{
					"result": *(*string)(unsafe.Pointer(&b)),
				},
			}
			if err := r.Create(context.TODO(), cm); err != nil {
				return ctrl.Result{}, err
			}
		}
	}

	jobList := &batchv1.JobList{}
	if err := r.List(context.TODO(), jobList); err != nil {
		return ctrl.Result{}, err
	}
	log.Info("查询job列表...")
	for _, job := range jobList.Items {
		// 这边只更新job的状态
		jobCm := &corev1.ConfigMap{}
		err := r.Get(ctx, types.NamespacedName{Name: job.Name, Namespace: memcached.Namespace}, jobCm)
		if err != nil {
			log.Info("找不到名称为" + job.Name + "的configmap")
			continue
		}

		jr := JobResult{}
		if err = yaml.Unmarshal([]byte(jobCm.Data["result"]), &jr); err != nil {
			log.Info("解析错误")
			continue
		}

		var updateFlag bool
		//一个job当前只会启动一个pod
		if job.Status.Succeeded > 0 && jr.Status != "success" {
			jr.Status = "success"
			updateFlag = true
		}
		if job.Status.Failed > 0 && jr.Status != "fail" {
			jr.Status = "fail"
			updateFlag = true
		}
		if updateFlag {
			if err := r.Update(context.TODO(), jobCm); err != nil {
				return ctrl.Result{}, err
			}
		}

		log.Info("job名字是:" + job.Name)
		log.Info("succeeded", "succeeded", job.Status.Succeeded)
		log.Info("active", "active", job.Status.Active)
		log.Info("failed", "failed", job.Status.Failed)
	}

	return ctrl.Result{}, nil
}

type JobResult struct {
	Status string `yaml:"status"`
}

func (r *MemcachedReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&cachev1alpha1.Memcached{}).
		Owns(&v1beta1.CronJob{}).Complete(r)
}

func (r *MemcachedReconciler) SetupWithManager3(mgr ctrl.Manager) *builder.Builder {
	return ctrl.NewControllerManagedBy(mgr).
		For(&cachev1alpha1.Memcached{}).
		Owns(&v1beta1.CronJob{})
}

func (r *MemcachedReconciler) SetupWithManager2(mgr ctrl.Manager, log logr.Logger) *ctrl.Builder {
	b := ctrl.NewControllerManagedBy(mgr).For(&cachev1alpha1.Memcached{})
	b.Owns(&v1beta1.CronJob{}).Owns(&batchv1.Job{})
	b.WithEventFilter(predicate.Funcs{
		CreateFunc: func(createEvent event.CreateEvent) bool {
			if cronJob, ok := createEvent.Object.(*v1beta1.CronJob); ok {
				r.Log.Info("cronjob resource 创建了", "name", cronJob.Name,
					"namespace", cronJob.Namespace)
				return false
			}
			if job, ok := createEvent.Object.(*batchv1.Job); ok {
				r.Log.Info("job resource 创建了", "name", job.Name,
					"namespace", job.Namespace)
				return false
			}
			return true
		},
		UpdateFunc: func(updateEvent event.UpdateEvent) bool {
			//object, err := meta.Accessor(e.ObjectNew)
			//if err != nil {
			//	return false
			//}
			//switch object.(type) {
			//case *v1beta1.CronJob:
			//	log.Info("job 更新了。。。", "job name", 22)
			//	return true
			//case *batchv1.Job:
			//	log.Info("cronjob 更新了。。。", "cronjob name", 23)
			//	return true
			//}
			if cronJob, ok := updateEvent.ObjectNew.(*v1beta1.CronJob); ok {
				if reflect.DeepEqual(cronJob.Spec, updateEvent.ObjectOld.(*v1beta1.CronJob).Spec) &&
					reflect.DeepEqual(cronJob.Status, updateEvent.ObjectOld.(*v1beta1.CronJob).Status) {
					log.Info("cron job 没啥变化的。。。", "cronjob name", cronJob.Name)
					return false
				} else {
					log.Info("cron job 变化了！！！", "cronjob name", cronJob.Name)
					return true
				}
			}
			if job, ok := updateEvent.ObjectNew.(*batchv1.Job); ok {
				if reflect.DeepEqual(job.Spec, updateEvent.ObjectOld.(*batchv1.Job).Spec) &&
					reflect.DeepEqual(job.Status, updateEvent.ObjectOld.(*batchv1.Job).Status) {
					log.Info("job 没啥变化的。。。", "cronjob name", job.Name)
					return false
				} else {
					log.Info("job 变化了！！！", "cronjob name", job.Name)
					return true
				}
			}
			return true
		},
	})
	return b
	//return ctrl.NewControllerManagedBy(mgr).
	//	For(&cachev1alpha1.Memcached{}).
	//	Owns(&v1beta1.CronJob{}, builder.WithPredicates(r.cronjobPredicate())).
	//	Owns(&batchv1.Job{}, builder.WithPredicates(r.jobPredicate()))
}

func (r *MemcachedReconciler) SetupWithManager1(mgr ctrl.Manager) *builder.Builder {
	return ctrl.NewControllerManagedBy(mgr).
		For(&cachev1alpha1.Memcached{}).
		Owns(&v1beta1.CronJob{}, builder.WithPredicates(r.cronjobPredicate())).
		Owns(&batchv1.Job{}, builder.WithPredicates(r.jobPredicate()))
}

func (r *MemcachedReconciler) cronjobPredicate() predicate.Predicate {
	return predicate.Funcs{
		UpdateFunc: func(updateEvent event.UpdateEvent) bool {
			if cronjob, ok := updateEvent.ObjectNew.(*v1beta1.CronJob); ok {
				if reflect.DeepEqual(cronjob.Spec, updateEvent.ObjectOld.(*v1beta1.CronJob).Spec) &&
					reflect.DeepEqual(cronjob.Status, updateEvent.ObjectOld.(*v1beta1.CronJob).Status) {
					return false
				}
				r.Log.Info("cronjob 更新了。。。", "job name", cronjob.Name)
			}
			return true
		},
	}
}

func (r *MemcachedReconciler) jobPredicate() predicate.Predicate {
	return predicate.Funcs{
		UpdateFunc: func(updateEvent event.UpdateEvent) bool {
			if job, ok := updateEvent.ObjectNew.(*batchv1.Job); ok {
				if reflect.DeepEqual(job.Spec, updateEvent.ObjectOld.(*batchv1.Job).Spec) &&
					reflect.DeepEqual(job.Status, updateEvent.ObjectOld.(*batchv1.Job).Status) {
					return false
				}
				r.Log.Info("job 更新了。。。", "job name", job.Name)
			}
			return true
		},
	}
}
