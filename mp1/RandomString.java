

class RandomString{
    public static String getRandomString(int length){
        String str = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
        
        StringBuffer sb = new StringBuffer();
        for(int i=0;i<length;i++){
        int number = (int)(Math.random()*62);
        sb.append(str.charAt(number));
        }
        return sb.toString();
    }
    public static void main(String[] args){
        RandomString r = new RandomString();
        System.out.println(r.getRandomString(10));
    }
}