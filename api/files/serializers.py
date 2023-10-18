from rest_framework import serializers

from accounts.serializers import UserSerializer

from .models import UploadedFile


class FileSerializer(serializers.ModelSerializer):
    """
    Serializer for File model
    """
    user = UserSerializer(read_only=True)
    url = serializers.FileField()

    class Meta:
        model = UploadedFile
        fields = '__all__'

    def get_file(self, document):
        request = self.context.get('request')
        file_url = document.file.url
        return request.build_absolute_uri(file_url)
