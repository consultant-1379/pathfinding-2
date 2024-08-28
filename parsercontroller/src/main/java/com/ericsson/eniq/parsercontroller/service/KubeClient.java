package com.ericsson.eniq.parsercontroller.service;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.lang.Nullable;
import org.springframework.stereotype.Service;

import io.fabric8.kubernetes.api.model.Condition;
import io.fabric8.kubernetes.api.model.ContainerPort;
import io.fabric8.kubernetes.api.model.EnvVar;
import io.fabric8.kubernetes.api.model.HostPathVolumeSource;
import io.fabric8.kubernetes.api.model.Quantity;
import io.fabric8.kubernetes.api.model.ResourceRequirements;
import io.fabric8.kubernetes.api.model.Volume;
import io.fabric8.kubernetes.api.model.VolumeMount;
import io.fabric8.kubernetes.api.model.batch.Job;
import io.fabric8.kubernetes.api.model.batch.JobBuilder;
import io.fabric8.kubernetes.api.model.batch.JobCondition;
import io.fabric8.kubernetes.client.Config;
import io.fabric8.kubernetes.client.ConfigBuilder;
import io.fabric8.kubernetes.client.DefaultKubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClient;

@Service
public class KubeClient {
	
	private static final Logger LOG = LogManager.getLogger(KubeClient.class);
	
	@Value("${k8s.master.url}")
	private String masterUrl;
	
	@Value("${k8s.job.namespace}")
	private String jobNameSpace;
	
	@Value("${k8s.job.maxJobAge}")
	private int maxJobAge;
	
	private static final String COMPLETED = "Completed";
	private static final String FAILED = "Failed";
	private static final String TRUE = "True";
	private static final String FALSE = "False";
	private static final String UNKNOWN = "Unknown";
	
	public enum JobStatus {
		NOT_EXIST,
		EXIST_COMPLETED,
		EXIST_NOT_COMPLETED
	}
	
	@Nullable 
	public Job triggerParser(String jobFile) {
		Job job = null;
		Config config = new ConfigBuilder().withMasterUrl(masterUrl).build();
		try(KubernetesClient client = new DefaultKubernetesClient(config)) {
			//client.load(new  FileInputStream(new File(""))).fromServer().get();
			job = client.batch().jobs().load(new  FileInputStream(new File(jobFile))).createOrReplace();
			//client.batch().jobs().inNamespace("pathfinding2").create(buildJob());
		} catch (Exception e) {
			LOG.log(Level.WARN, "Exception on attempt to trigger short-lived-job using file : " +jobFile, e);
		}
		return job;
	}
		
	public JobStatus getJobStatus(String jobFile) {
		JobStatus jobExists = JobStatus.NOT_EXIST;
		Config config = new ConfigBuilder().withMasterUrl(masterUrl).build();
		try(KubernetesClient client = new DefaultKubernetesClient(config)) {
			Job job = client.batch().jobs().load(new  FileInputStream(new File(jobFile))).get();
			LOG.log(Level.INFO,"Job obtained by loading the file  {} is : {}" ,jobFile, job );
			String jobName = job.getMetadata().getName();
			Job actualJob = client.batch().jobs().inNamespace(jobNameSpace).withName(jobName).get();
			LOG.log(Level.INFO,"Job obtained from cluster is : {}" ,actualJob );
			if (actualJob != null && actualJob.getStatus() != null) {
				String completionTime = actualJob.getStatus().getCompletionTime();
				LOG.log(Level.INFO, "Job Exists, job name = {}",actualJob.getMetadata().getName());
				if (completionTime == null) {
					jobExists = JobStatus.EXIST_NOT_COMPLETED;
				} else {
					jobExists = JobStatus.EXIST_COMPLETED;
					LOG.log(Level.INFO, "Existing job is in completed state, completion time = {}",completionTime);
				}
			} else {
				LOG.log(Level.INFO," Job does not exist for file : {}", jobFile);
			}
		} catch (FileNotFoundException e) {
			LOG.log(Level.WARN, "Exception on attempt to get the job from server", e);
		}
		return jobExists;
	}
	
	public List<Job> getCompletedJobs() {
		List<Job> completedJobs = new ArrayList<>();
		Config config = new ConfigBuilder().withMasterUrl(masterUrl).build();
		try (KubernetesClient client = new DefaultKubernetesClient(config)) {
			List<Job> jobs = client.batch().jobs().inNamespace(jobNameSpace).list().getItems();
			LOG.log(Level.INFO, "Available jobs in namespace = {}", jobs);
			if (jobs == null) {
				return completedJobs;
			}
			for (Job job : jobs) {
				if (isJobEligibleForDelete(job)) {
					completedJobs.add(job);
				}
			}
			LOG.log(Level.INFO, "completed Jobs : {}",completedJobs);
		}
		return completedJobs;
	}
		
	private boolean isJobEligibleForDelete(Job job) {
		LOG.log(Level.INFO, "Checking for job eligibility for delete, job = {} ",job);
		boolean isCompleted = false;
		String completionTimeString = job.getStatus().getCompletionTime();
		if(completionTimeString != null && !completionTimeString.isEmpty()) {
			Instant completionTime = Instant.parse(completionTimeString);
			LOG.log(Level.INFO, "Completion time = {} ",completionTime);
			Instant now = Instant.now();
			if ((now.getEpochSecond() - completionTime.getEpochSecond()) >= maxJobAge) {
				isCompleted = true;
				LOG.log(Level.INFO, "Job is eligible for deletion, job = {} ",job);
			}
		}
		return isCompleted;
		
	}
	
	public boolean clearJobs(List<Job> jobs) {
		LOG.log(Level.INFO, "Jobs to be cleared = {} ",jobs);
		Config config = new ConfigBuilder().withMasterUrl(masterUrl).build();
		try(KubernetesClient client = new DefaultKubernetesClient(config)) {
			return client.batch().jobs().delete(jobs);
		}
	}
	
	public boolean clearJobs(Job... jobs) {
		LOG.log(Level.INFO, "Jobs to be cleared : " +jobs);
		Config config = new ConfigBuilder().withMasterUrl(masterUrl).build();
		try(KubernetesClient client = new DefaultKubernetesClient(config)) {
			return client.batch().jobs().delete(jobs);
		}
	}
	
	public boolean clearJob(String jobFile) {
		Boolean result = false;
		Config config = new ConfigBuilder().withMasterUrl(masterUrl).build();
		try(KubernetesClient client = new DefaultKubernetesClient(config)) {
			Job job = client.batch().jobs().load(new  FileInputStream(new File(jobFile))).get();
			LOG.log(Level.INFO,"Job obtained by loading the file  {} is : {}" ,jobFile, job );
			String jobName = job.getMetadata().getName();
			result =  client.batch().jobs().inNamespace(jobNameSpace).withName(jobName).delete();
		} catch (FileNotFoundException e) {
			LOG.log(Level.WARN, "Exception while trying to clear job for give file : "+jobFile, e);
		}
		return result;
	}

	
	
	private boolean isCompleted2(Job job) {
		boolean isCompleted = true;
		for (JobCondition condition : job.getStatus().getConditions()) {
			String status = condition.getStatus();
			if (status.equals(COMPLETED) || status.equals(FAILED)) {
				isCompleted = isCompleted && Boolean.valueOf(true);
			} else {
				isCompleted = false;
			}
		}
		return isCompleted;
	}
	
	
	public Job buildJob() {
		
		List<EnvVar> env = new ArrayList<>();
		EnvVar var1 = new EnvVar();
		var1.setName("APP_PORT");
		var1.setValue("9887");
		env.add(var1);
		EnvVar var2 = new EnvVar();
		var2.setName("APP_HOME");
		var2.setValue("/root/parser");
		env.add(var2);
		EnvVar var3 = new EnvVar();
		var3.setName("APP_STARTUP");
		var3.setValue("com.ericsson.eniq.sbkafka.SbkafkaApplication");
		env.add(var3);
		VolumeMount volMount = new VolumeMount();
		volMount.setMountPath("/root/mdc/");
		volMount.setName("host-fs");
		ResourceRequirements resReq = new ResourceRequirements();
		resReq.setRequests(Collections.singletonMap("cpu", new Quantity("1")));
		ContainerPort cPort = new ContainerPort();
		cPort.setHostPort(7878);
		Volume vol = new Volume();
		HostPathVolumeSource hPath = new HostPathVolumeSource();
		hPath.setPath("/root/mdc");
		hPath.setType("Directory");
		vol.setHostPath(hPath);
				
		return new JobBuilder()
	            .withApiVersion("batch/v1")
	            .withNewMetadata()
	            .withName("sbkafka-job")
	            .withLabels(Collections.singletonMap("app", "sbkafka-job"))
	            .endMetadata()
	            .withNewSpec().withParallelism(1)
	            .withNewTemplate()
	            .withNewMetadata()
	            .withLabels(Collections.singletonMap("app", "sbkafka-job"))
	            .endMetadata()
	            .withNewSpec()
	            .addNewContainer()
	            .withName("sbkafka-job")
	            .withImage("10.45.194.77:5000/sbkafka-job:1.0")
	            .withImagePullPolicy("Always")
	            .withEnv(env)
	            .withVolumeMounts(volMount)
	            .withResources(resReq)
	            .withPorts(Collections.singletonList(cPort))
	            .endContainer()
	            .withVolumes(Collections.singletonList(vol))
	            .withRestartPolicy("Never")
	            .endSpec()
	            .endTemplate()
	            .endSpec()
	            .build();
		
	}

}
