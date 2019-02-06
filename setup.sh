ssh-keygen -t rsa -b 4096 -C "your_email@example.com"
for i in `seq 1 9`;
do
	cat ~/.ssh/id_rsa.pub | ssh zli104@fa18-cs425-g73-0${i}.cs.illinois.edu 'cat >> .ssh/authorized_keys'
	ssh zli104@fa18-cs425-g73-0${i}.cs.illinois.edu "chmod 700 .ssh; chmod 600 .ssh/authorized_keys"
done

cat ~/.ssh/id_rsa.pub | ssh zli104@fa18-cs425-g73-10.cs.illinois.edu 'cat >> .ssh/authorized_keys'
ssh zli104@fa18-cs425-g73-10.cs.illinois.edu "chmod 700 .ssh; chmod 600 .ssh/authorized_keys"




wait





