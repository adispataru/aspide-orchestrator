/*
 * Copyright 2017 Institute e-Austria Timisoara.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package alien4cloud.aspide;

import alien4cloud.paas.model.InstanceStatus;
import com.jcraft.jsch.*;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.io.InputStream;
import java.io.StringBufferInputStream;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by adrian on 10.10.2017.
 */
@Component
public class ASPIDEOrcClient {

    private static final String SINFO_COMMAND = "squeue  -j jobid --steps";
    private String COMPILE_COMMAND = "cat path > program";
    private String SBATCH_COMMAND = "cd path && sbatch ./program.sbatch";
    private String SCANCEL_COMMAND = "scancel jobid";

    public boolean compileImplementationSSH(Configuration configuration, String workflowId, String fileName, String impl){
        String SLURM_URL = configuration.getUrl();
        String port = "22";
        int colonPos = SLURM_URL.indexOf(":");
        if(colonPos > -1 ){
            port = SLURM_URL.substring(colonPos);
            SLURM_URL = SLURM_URL.substring(0, colonPos);
        }
        String SLURM_USER = configuration.getUser();
        String SLURM_PASS = configuration.getPassword();
        String DCE_RUNTIME_PATH = configuration.getDcePath();
        String codeFileName = fileName + ".dcx";

        JSch jsch = new JSch();
        try {
            Session session = jsch.getSession(SLURM_USER, SLURM_URL, Integer.parseInt(port));
            session.setConfig("StrictHostKeyChecking", "no");
            session.setConfig("PreferredAuthentications", "password");
            session.setPassword(SLURM_PASS);
            session.connect(2000);
            System.out.println("Connected to session successfully");
            Channel channel = session.openChannel("sftp");
            channel.connect();
            System.out.println("Connected to Channel successfully");
            ChannelSftp sftpChannel = (ChannelSftp) channel;

            // this will read file with the name test.txt and write to remote directory
            String finalPath = DCE_RUNTIME_PATH + "/" + workflowId + "/";

            String[] folders = finalPath.split( "/" );
            sftpChannel.cd("/");
            for ( String folder : folders ) {
                if ( folder.length() > 0 ) {
                    try {
                        sftpChannel.cd( folder );
                    }
                    catch ( SftpException e ) {
                        sftpChannel.mkdir( folder );
                        sftpChannel.cd( folder );
                    }
                }
            }
//            sftpChannel.cd(finalPath);

            sftpChannel.put(new StringBufferInputStream(impl), codeFileName);

            sftpChannel.exit();

            String command = COMPILE_COMMAND.replace("path", finalPath + codeFileName).replace("program", finalPath + fileName);
            Channel exec=session.openChannel("exec");
            ((ChannelExec)exec).setCommand(command);

            // X Forwarding
            // channel.setXForwarding(true);

            //channel.setInputStream(System.in);
            exec.setInputStream(null);

            //channel.setOutputStream(System.out);

            //FileOutputStream fos=new FileOutputStream("/tmp/stderr");
            //((ChannelExec)channel).setErrStream(fos);
//            ((ChannelExec)exec).setErrStream(System.err);

            InputStream in=exec.getInputStream();

            exec.connect();

            byte[] tmp=new byte[1024];
            int exitStatus = 0;
            while(true){
                while(in.available()>0){
                    int i=in.read(tmp, 0, 1024);
                    if(i<0)break;
                    System.out.print(new String(tmp, 0, i));
                }
                if(exec.isClosed()){
                    if(in.available()>0) continue;
                    exitStatus = exec.getExitStatus();
                    break;
                }
                try{Thread.sleep(1000);}catch(Exception ee){}
            }
            exec.disconnect();
            session.disconnect();
            return exitStatus == 0;

        } catch (JSchException e) {
            e.printStackTrace();
        } catch (SftpException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return false;
    }

    public boolean copySLURMScript(Configuration configuration, String fileName, String script){
        String SLURM_URL = configuration.getUrl();
        String port = "22";
        int colonPos = SLURM_URL.indexOf(":");
        if(colonPos > -1 ){
            port = SLURM_URL.substring(colonPos);
            SLURM_URL = SLURM_URL.substring(0, colonPos);
        }
        String SLURM_USER = configuration.getUser();
        String SLURM_PASS = configuration.getPassword();
        String DCE_RUNTIME_PATH = configuration.getDcePath();
//        String codeFileName = fileName + ".dcx";

        JSch jsch = new JSch();
        try {
            Session session = jsch.getSession(SLURM_USER, SLURM_URL, Integer.parseInt(port));
            session.setConfig("StrictHostKeyChecking", "no");
            session.setConfig("PreferredAuthentications", "password");
            session.setPassword(SLURM_PASS);
            session.connect(2000);
            System.out.println("Connected to session successfully");
            Channel channel = session.openChannel("sftp");
            channel.connect();
            System.out.println("Connected to Channel successfully");
            ChannelSftp sftpChannel = (ChannelSftp) channel;

            // this will read file with the name test.txt and write to remote directory
            String finalPath = DCE_RUNTIME_PATH + "/" + fileName + "/";
            sftpChannel.cd(finalPath);

            sftpChannel.put(new StringBufferInputStream(script), fileName+".sbatch");

            sftpChannel.exit();


        } catch (JSchException e) {
            e.printStackTrace();
        } catch (SftpException e) {
            e.printStackTrace();
        }
        return false;
    }


    public String sBatch(Configuration configuration, String workflowId) {
        String SLURM_URL = configuration.getUrl();
        String port = "22";
        int colonPos = SLURM_URL.indexOf(":");
        if(colonPos > -1 ){
            port = SLURM_URL.substring(colonPos);
            SLURM_URL = SLURM_URL.substring(0, colonPos);
        }
        String SLURM_USER = configuration.getUser();
        String SLURM_PASS = configuration.getPassword();
        String DCE_RUNTIME_PATH = configuration.getDcePath();

        JSch jsch = new JSch();
        try {
            Session session = jsch.getSession(SLURM_USER, SLURM_URL, Integer.parseInt(port));
            session.setConfig("StrictHostKeyChecking", "no");
            session.setConfig("PreferredAuthentications", "password");
            session.setPassword(SLURM_PASS);
            session.connect(2000);
            System.out.println("Connected to session successfully");

            String finalPath = DCE_RUNTIME_PATH + "/" + workflowId + "/";
            String command = SBATCH_COMMAND.replace("path", finalPath).replace("program", workflowId);
            Channel exec=session.openChannel("exec");
            ((ChannelExec)exec).setCommand(command);

            // X Forwarding
            // channel.setXForwarding(true);

            //channel.setInputStream(System.in);
            exec.setInputStream(null);

            //channel.setOutputStream(System.out);

            //FileOutputStream fos=new FileOutputStream("/tmp/stderr");
            //((ChannelExec)channel).setErrStream(fos);
//            ((ChannelExec)exec).setErrStream(System.err);

            InputStream in=exec.getInputStream();

            exec.connect();

            byte[] tmp=new byte[1024];
            StringBuilder sb = new StringBuilder();
            int exitStatus = 0;
            while(true){
                while(in.available()>0){
                    int i=in.read(tmp, 0, 1024);
                    if(i<0)break;
                    String s = new String(tmp);
                    s = s.substring(0, s.lastIndexOf("\n"));
                    sb.append(s);
                }
                if(exec.isClosed()){
                    if(in.available()>0) continue;
                    exitStatus = exec.getExitStatus();
                    break;
                }
                try{Thread.sleep(1000);}catch(Exception ee){}
            }
            exec.disconnect();
            session.disconnect();
            return sb.toString();

        } catch (JSchException | IOException e) {
            return e.getMessage();
        }

    }

    public void cancelJob(Configuration configuration, String id) {

        String SLURM_URL = configuration.getUrl();
        String port = "22";
        int colonPos = SLURM_URL.indexOf(":");
        if(colonPos > -1 ){
            port = SLURM_URL.substring(colonPos);
            SLURM_URL = SLURM_URL.substring(0, colonPos);
        }
        String SLURM_USER = configuration.getUser();
        String SLURM_PASS = configuration.getPassword();
        String DCE_RUNTIME_PATH = configuration.getDcePath();

        JSch jsch = new JSch();
        try {
            Session session = jsch.getSession(SLURM_USER, SLURM_URL, Integer.parseInt(port));
            session.setConfig("StrictHostKeyChecking", "no");
            session.setConfig("PreferredAuthentications", "password");
            session.setPassword(SLURM_PASS);
            session.connect(2000);
            System.out.println("Connected to session successfully");


            String command = SCANCEL_COMMAND.replace("jobid", id);
            Channel exec=session.openChannel("exec");
            ((ChannelExec)exec).setCommand(command);

            // X Forwarding
            // channel.setXForwarding(true);

            //channel.setInputStream(System.in);
            exec.setInputStream(null);

            //channel.setOutputStream(System.out);

            //FileOutputStream fos=new FileOutputStream("/tmp/stderr");
            //((ChannelExec)channel).setErrStream(fos);
//            ((ChannelExec)exec).setErrStream(System.err);

            InputStream in=exec.getInputStream();

            exec.connect();

            byte[] tmp=new byte[1024];
            StringBuilder sb = new StringBuilder();
            int exitStatus = 0;
            while(true){
                while(in.available()>0){
                    int i=in.read(tmp, 0, 1024);
                    if(i<0)break;
                    String s = new String(tmp);
                    s = s.substring(0, s.lastIndexOf("\n"));
                    sb.append(s);
                }
                if(exec.isClosed()){
                    if(in.available()>0) continue;
                    exitStatus = exec.getExitStatus();
                    break;
                }
                try{Thread.sleep(1000);}catch(Exception ee){}
            }
            exec.disconnect();
            session.disconnect();


        } catch (JSchException | IOException e) {
            System.err.println(e.getMessage());
        }

    }

    public Map<String, String> getInfo(Configuration configuration, String workflowId, String taskIndex) {
        String SLURM_URL = configuration.getUrl();
        String port = "22";
        int colonPos = SLURM_URL.indexOf(":");
        if(colonPos > -1 ){
            port = SLURM_URL.substring(colonPos);
            SLURM_URL = SLURM_URL.substring(0, colonPos);
        }
        String SLURM_USER = configuration.getUser();
        String SLURM_PASS = configuration.getPassword();
        String DCE_RUNTIME_PATH = configuration.getDcePath();

        JSch jsch = new JSch();
        try {
            Session session = jsch.getSession(SLURM_USER, SLURM_URL, Integer.parseInt(port));
            session.setConfig("StrictHostKeyChecking", "no");
            session.setConfig("PreferredAuthentications", "password");
            session.setPassword(SLURM_PASS);
            session.connect(2000);
            System.out.println("Connected to session successfully");


            String command = SINFO_COMMAND.replace("jobid", workflowId);
            Channel exec=session.openChannel("exec");
            ((ChannelExec)exec).setCommand(command);

            // X Forwarding
            // channel.setXForwarding(true);

            //channel.setInputStream(System.in);
            exec.setInputStream(null);

            //channel.setOutputStream(System.out);

            //FileOutputStream fos=new FileOutputStream("/tmp/stderr");
            //((ChannelExec)channel).setErrStream(fos);
//            ((ChannelExec)exec).setErrStream(System.err);

            InputStream in=exec.getInputStream();

            exec.connect();

            byte[] tmp=new byte[1024];
            StringBuilder sb = new StringBuilder();
            int exitStatus = 0;
            while(true){
                while(in.available()>0){
                    int i=in.read(tmp, 0, 1024);
                    if(i<0)break;
                    String s = new String(tmp);
                    s = s.substring(0, s.lastIndexOf("\n"));
                    sb.append(s);
                }
                if(exec.isClosed()){
                    if(in.available()>0) continue;
                    exitStatus = exec.getExitStatus();
                    break;
                }
                try{Thread.sleep(1000);}catch(Exception ee){}
            }
            exec.disconnect();
            session.disconnect();

            String[] tokens = sb.toString().split("\n");
            if(tokens.length > 1){

                String[] fields = tokens[0].split("\\s+");

                for(int i = 1; i < tokens.length; i++){
                    String[] values = tokens[i].split("\\s+");
                    if(values[1].equals(String.format("%s.%s", workflowId, taskIndex))) {
                        Map<String, String> result = new HashMap<>();
                        for (int j = 1; j < fields.length; j++) {
                            result.put(fields[j], values[j]);
                        }
                        return result;
                    }else{
                        //check if current running step is greater than queried step
                        Double currentStep = Double.parseDouble(values[1]);
                        String s = String.format("%s.%s", workflowId, taskIndex); // jobid.step
                        Double queriedStep = Double.parseDouble(s);
                        if(currentStep > queriedStep){
                            Map<String, String> result = new HashMap<>();
                            result.put("status", InstanceStatus.SUCCESS.name());
                            return result;
                        }
                    }
                }
                Map<String, String> result = new HashMap<>();
                result.put("status", InstanceStatus.MAINTENANCE.name());

                return result;


            }else{
                Map<String, String> result = new HashMap<>();
                result.put("status", InstanceStatus.SUCCESS.name());
                return result;
            }





        } catch (JSchException | IOException e) {
            System.err.println(e.getMessage());
        }
        return null;
    }
}
