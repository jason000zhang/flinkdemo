package com.calabar.flinkDemo.wordCount;
import com.sun.tools.attach.AgentInitializationException;
import com.sun.tools.attach.AgentLoadException;
import com.sun.tools.attach.AttachNotSupportedException;
import com.sun.tools.attach.VirtualMachine;

import java.io.IOException;

public class Attacher {
    public static void main(String[] args) throws AttachNotSupportedException, IOException, AgentLoadException, AgentInitializationException {
        // 传入目标 JVM pid
        VirtualMachine vm = VirtualMachine.attach("15852");
        vm.loadAgent("E:\\workspaces_new\\flinkDemo-master\\target\\flinkDemo-1.0-SNAPSHOT.jar");
    }
}
