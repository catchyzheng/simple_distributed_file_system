import org.grep4j.core.model.Profile;
import org.grep4j.core.model.ProfileBuilder;
import org.grep4j.core.result.GrepResult;
import org.grep4j.core.result.GrepResults;

import static org.grep4j.core.Grep4j.constantExpression;
import static org.grep4j.core.Grep4j.grep;
import static org.grep4j.core.fluent.Dictionary.executing;
import static org.grep4j.core.fluent.Dictionary.on;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

public class mp1 {

    //Obtaining the global result
    public static void main(String [] args){
        //set a profile for each log on VM
        //Profile localProfile = ProfileBuilder.newBuilder().name("Local server log").filePath("/home/zli104/vm1.log").onLocalhost().build();
        Profile localProfile = ProfileBuilder.newBuilder().name("Local server log").filePath("/home/zli104/vm1.log").onRemotehost("fa18-cs425-g73-01.cs.illinois.edu").credentials("zli104", "Lzrgueen1996").build();
        Profile remoteProfile_2 = ProfileBuilder.newBuilder().name("Remote server log2").filePath("/home/zli104/vm2.log").onRemotehost("fa18-cs425-g73-02.cs.illinois.edu").credentials("zli104", "Lzrgueen1996").build();
        Profile remoteProfile_3 = ProfileBuilder.newBuilder().name("Remote server log3").filePath("/home/zli104/vm3.log").onRemotehost("fa18-cs425-g73-03.cs.illinois.edu").credentials("zli104", "Lzrgueen1996").build();
        Profile remoteProfile_4 = ProfileBuilder.newBuilder().name("Remote server log4").filePath("/home/zli104/vm4.log").onRemotehost("fa18-cs425-g73-04.cs.illinois.edu").credentials("zli104", "Lzrgueen1996").build();
        Profile remoteProfile_5 = ProfileBuilder.newBuilder().name("Remote server log5").filePath("/home/zli104/vm5.log").onRemotehost("fa18-cs425-g73-05.cs.illinois.edu").credentials("zli104", "Lzrgueen1996").build();
        Profile remoteProfile_6 = ProfileBuilder.newBuilder().name("Remote server log6").filePath("/home/zli104/vm6.log").onRemotehost("fa18-cs425-g73-06.cs.illinois.edu").credentials("zli104", "Lzrgueen1996").build();
        Profile remoteProfile_7 = ProfileBuilder.newBuilder().name("Remote server log7").filePath("/home/zli104/vm7.log").onRemotehost("fa18-cs425-g73-07.cs.illinois.edu").credentials("zli104", "Lzrgueen1996").build();
        Profile remoteProfile_8 = ProfileBuilder.newBuilder().name("Remote server log8").filePath("/home/zli104/vm8.log").onRemotehost("fa18-cs425-g73-08.cs.illinois.edu").credentials("zli104", "Lzrgueen1996").build();
        Profile remoteProfile_9 = ProfileBuilder.newBuilder().name("Remote server log9").filePath("/home/zli104/vm9.log").onRemotehost("fa18-cs425-g73-09.cs.illinois.edu").credentials("zli104", "Lzrgueen1996").build();
        Profile remoteProfile_10 = ProfileBuilder.newBuilder().name("Remote server log10").filePath("/home/zli104/vm10.log").onRemotehost("fa18-cs425-g73-10.cs.illinois.edu").credentials("zli104", "Lzrgueen1996").build();

        //set a pattern which need to query
        String str = "a05lVDbzxy1RfxbkGxqvSYRs+6Gm8QIp91v8ri2SlhVnxf0zc2ygePrg8aUf9xxH";

        //grep results distributedly
        GrepResults results = grep(constantExpression(str), on(localProfile, remoteProfile_2, remoteProfile_3, remoteProfile_4,
        remoteProfile_5, remoteProfile_6, remoteProfile_7, remoteProfile_8, remoteProfile_9, remoteProfile_10));


        System.out.println("Total lines found : " + results.totalLines());
        System.out.println("Total Execution Time : " + results.getExecutionTime());

        //processing the single grep result for each profile
        for (GrepResult singleResult : results) {
            //System.out.println(singleResult.getProfileName());
            System.out.println(singleResult.getFileName());
            System.out.println("line found : " + singleResult.totalLines());
            System.out.println("execution time: " + singleResult.getExecutionTime());
            if(singleResult.getFileName()!="/home/zli104/vm9.log")
                assertEquals("results should be 7", 7, (singleResult).totalLines());
            else
                assertEquals("results should be 8", 8, (singleResult).totalLines());
        }
        // unit test for all 
        assertEquals("results should be 71", 71, (results).totalLines());
    }
}
