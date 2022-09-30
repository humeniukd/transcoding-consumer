# wf
AWS elastic beanstalk transcoding consumer based on [custom FFmpeg](https://patchwork.ffmpeg.org/project/ffmpeg/patch/20190107143115.101095-1-dhumeniuk@google.com) build.  
AWS: Get file from S3 storage, probe audio getting length, convert it to mp3 while sending progress to the SQS queue and upload results back to S3
